package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/httpapi"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/mqtt"
	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/internal/replication"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type replicationRunner interface {
	Run(ctx context.Context) error
}

type appFactory struct {
	newStorageManager    func(cfg storage.Config) (*storage.Manager, error)
	newMetadataStore     func(cfg api.BrokerConfig) (*metadata.Store, error)
	newController        func(cfg api.BrokerConfig, topics map[string]metadata.TopicState) (controller.MetadataStore, error)
	newOffsetStore       func(dataDir string) (*broker.OffsetStore, error)
	newBroker            func(cfg api.BrokerConfig, store broker.Storage, offsets *broker.OffsetStore, meta *metadata.Store, cluster controller.MetadataStore, initialMeta *api.ClusterMetadata) (*broker.Broker, error)
	newNetServer         func(addr string, brokerAPI netproto.BrokerAPI) (*netproto.Server, error)
	newMQTTServer        func(addr string, brokerAPI mqtt.BrokerAPI) (*mqtt.Server, error)
	newHTTPHandler       func(b *broker.Broker, cfg api.BrokerConfig, ctrl controller.MetadataStore) *httpapi.Handler
	startHTTPServer      func(ctx context.Context, addr string, ready func() bool, register func(*http.ServeMux), onStarted func()) error
	newReplicator        func() *replication.BinaryReplicator
	newReplicationRunner func(cfg api.BrokerConfig, store *storage.Manager, ctrl controller.MetadataStore, repl replication.Replicator) replicationRunner
	waitForSignal        func()
}

type startupOptions struct {
	cfg          api.BrokerConfig
	syncOnAppend bool
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	opts, err := parseStartupOptions(os.Args[1:])
	if err != nil {
		logger.Error("invalid startup options", "err", err)
		os.Exit(1)
	}

	if err := validateBrokerConfig(logger, opts.cfg); err != nil {
		logger.Error("config validation failed", "err", err)
		os.Exit(1)
	}

	if err := run(context.Background(), logger, opts, defaultAppFactory()); err != nil {
		logger.Error("broker stopped", "err", err)
		os.Exit(1)
	}
}

func defaultAppFactory() appFactory {
	return appFactory{
		newStorageManager: storage.NewManager,
		newMetadataStore:  metadata.NewStore,
		newController:     controller.NewController,
		newOffsetStore:    broker.NewOffsetStore,
		newBroker:         broker.NewBroker,
		newNetServer:      netproto.NewServer,
		newMQTTServer:     mqtt.NewServer,
		newHTTPHandler:    httpapi.New,
		startHTTPServer:   observability.StartHTTPServer,
		newReplicator:     replication.NewBinaryReplicator,
		newReplicationRunner: func(cfg api.BrokerConfig, store *storage.Manager, ctrl controller.MetadataStore, repl replication.Replicator) replicationRunner {
			return replication.NewManager(cfg, store, ctrl, repl)
		},
		waitForSignal: waitForSignal,
	}
}

func parseStartupOptions(args []string) (startupOptions, error) {
	fs := flag.NewFlagSet("mbd", flag.ContinueOnError)

	var (
		dataDir           = fs.String("data-dir", "data", "path to broker data directory")
		binaryAddr        = fs.String("bind", ":7912", "address for binary protocol listener")
		advertisedAddr    = fs.String("advertise", "", "advertised binary address for cluster metadata (host:port)")
		mqttAddr          = fs.String("mqtt", ":1883", "address for MQTT listener")
		httpAddr          = fs.String("http", ":8090", "address for metrics/health HTTP listener")
		brokerID          = fs.Int("broker-id", 1, "numeric broker id")
		replicationFactor = fs.Int("replication-factor", 1, "default replication factor for new topics")
		segmentBytes      = fs.Int64("segment-bytes", 64<<20, "max segment size before rotation")
		syncOnAppend      = fs.Bool("sync-on-append", true, "fsync log segment on append for durability")
		retentionBytes    = fs.Int64("retention-bytes", -1, "total retention budget in bytes (-1 for unlimited)")
		retentionHours    = fs.Int("retention-hours", 0, "retention by age in hours (0 disables time-based retention)")
		controllerMode    = fs.String("controller", "single", "controller mode: single or raft")
		raftDir           = fs.String("raft-dir", "", "directory for Raft state (empty = in-memory)")
		raftBind          = fs.String("raft-bind", "", "raft bind address for controller (host:port)")
		raftPeers         = fs.String("raft-peer", "", "comma-separated list of raft peer addresses")
		enableReplication = fs.Bool("replication", false, "enable follower replication (experimental)")
	)

	if err := fs.Parse(args); err != nil {
		return startupOptions{}, err
	}

	cfg := api.BrokerConfig{
		BrokerID:          *brokerID,
		ReplicationFactor: *replicationFactor,
		DataDir:           *dataDir,
		BinaryAddr:        *binaryAddr,
		AdvertisedAddr:    *advertisedAddr,
		MQTTAddr:          *mqttAddr,
		HTTPAddr:          *httpAddr,
		MaxSegmentBytes:   *segmentBytes,
		RetentionBytes:    *retentionBytes,
		ControllerMode:    *controllerMode,
		RaftDir:           *raftDir,
		RaftBindAddr:      *raftBind,
		Replication:       *enableReplication,
	}
	if *raftPeers != "" {
		cfg.RaftPeers = strings.Split(*raftPeers, ",")
	}

	if *retentionHours > 0 {
		cfg.RetentionTime = time.Duration(*retentionHours) * time.Hour
	}

	return startupOptions{
		cfg:          cfg,
		syncOnAppend: *syncOnAppend,
	}, nil
}

func validateBrokerConfig(logger *slog.Logger, cfg api.BrokerConfig) error {
	if cfg.ControllerMode != "raft" {
		return nil
	}

	if cfg.RaftBindAddr == "" {
		return fmt.Errorf("raft controller mode requires -raft-bind")
	}

	if len(cfg.RaftPeers) == 0 {
		return fmt.Errorf("raft controller mode requires at least one -raft-peer (including self)")
	}

	inPeers := false

	for _, peer := range cfg.RaftPeers {
		if peer == cfg.RaftBindAddr {
			inPeers = true
			break
		}
	}

	if !inPeers {
		logger.Warn("raft-bind not present in raft-peer list", "bind", cfg.RaftBindAddr)
	}

	return nil
}

func run(parentCtx context.Context, logger *slog.Logger, opts startupOptions, factory appFactory) error {
	cfg := opts.cfg

	var readyFlag atomic.Bool

	store, err := factory.newStorageManager(storage.Config{
		DataDir:         cfg.DataDir,
		MaxSegmentBytes: cfg.MaxSegmentBytes,
		IndexInterval:   1024,
		SyncOnAppend:    opts.syncOnAppend,
		SegmentMaxAge:   cfg.RetentionTime,
		MaxLogBytes:     cfg.RetentionBytes,
		// TODO: load index interval/segment age from config or flags.
	})
	if err != nil {
		return fmt.Errorf("storage init failed: %w", err)
	}
	defer closeWithLog(logger, "storage manager", store.Close)

	if err := store.Recover(parentCtx); err != nil {
		return fmt.Errorf("storage recover failed: %w", err)
	}

	metadataStore, err := factory.newMetadataStore(cfg)
	if err != nil {
		return fmt.Errorf("metadata store init failed: %w", err)
	}
	defer closeWithLog(logger, "metadata store", metadataStore.Close)

	recoveredTopics, err := metadataStore.RecoverTopics(parentCtx)
	if err != nil {
		return fmt.Errorf("metadata recover failed: %w", err)
	}

	ctrl, err := factory.newController(cfg, recoveredTopics.Topics)
	if err != nil {
		return fmt.Errorf("controller init failed: %w", err)
	}

	logger.Info("controller initialized", "mode", cfg.ControllerMode)

	brokerHost := cfg.AdvertisedAddr
	if brokerHost == "" {
		brokerHost = cfg.BinaryAddr
	}

	bInfo := api.BrokerInfo{
		BrokerID:       cfg.BrokerID,
		Host:           brokerHost,
		HTTPAddr:       advertisedHTTPAddr(cfg),
		ControllerAddr: cfg.RaftBindAddr,
	}
	if err := registerBrokerWithRaft(parentCtx, logger, ctrl, cfg, bInfo); err != nil {
		return fmt.Errorf("broker registration failed: %w", err)
	}

	logger.Info("broker registered in controller", "brokerID", bInfo.BrokerID, "host", bInfo.Host)

	metaSnapshot, err := ctrl.GetClusterMetadata(parentCtx)
	if err != nil {
		return fmt.Errorf("cluster metadata fetch failed: %w", err)
	}

	logger.Info("fetched initial cluster metadata", "version", metaSnapshot.Version, "partitions", len(metaSnapshot.Partitions))

	offsetStore, err := factory.newOffsetStore(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("offset store init failed: %w", err)
	}
	defer closeWithLog(logger, "offset store", offsetStore.Close)

	b, err := factory.newBroker(cfg, store, offsetStore, metadataStore, ctrl, &metaSnapshot)
	if err != nil {
		return fmt.Errorf("broker init failed: %w", err)
	}
	defer closeWithLog(logger, "broker", b.Close)

	netServer, err := factory.newNetServer(cfg.BinaryAddr, b)
	if err != nil {
		return fmt.Errorf("netproto init failed: %w", err)
	}
	defer closeWithLog(logger, "netproto server", netServer.Close)

	mqttServer, err := factory.newMQTTServer(cfg.MQTTAddr, b)
	if err != nil {
		return fmt.Errorf("mqtt init failed: %w", err)
	}
	defer closeWithLog(logger, "mqtt server", mqttServer.Close)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	reportRunError := func(component string, runErr error, errCh chan<- error) {
		if runErr == nil || errors.Is(runErr, context.Canceled) {
			return
		}

		readyFlag.Store(false)
		logger.Error(component, "err", runErr)

		select {
		case errCh <- fmt.Errorf("%s: %w", component, runErr):
		default:
		}

		cancel()
	}

	if err := b.StartClusterMetadataWatcher(ctx); err != nil {
		return fmt.Errorf("cluster metadata watcher init failed: %w", err)
	}

	errCh := make(chan error, 4)

	if cfg.Replication {
		rep := factory.newReplicator()
		replMgr := factory.newReplicationRunner(cfg, store, ctrl, rep)

		go func() {
			logger.Info("replication manager starting", "mode", cfg.ControllerMode)
			reportRunError("replication manager stopped", replMgr.Run(ctx), errCh)
		}()
	}

	ready := readyFlag.Load

	go func() {
		apiHandler := factory.newHTTPHandler(b, cfg, ctrl)
		reportRunError("http server stopped", factory.startHTTPServer(ctx, cfg.HTTPAddr, ready, apiHandler.Register, nil), errCh)
	}()
	go func() {
		reportRunError("binary server stopped", netServer.ListenAndServe(ctx), errCh)
	}()
	go func() {
		reportRunError("mqtt server stopped", mqttServer.ListenAndServe(ctx), errCh)
	}()

	readyFlag.Store(true)

	signalCh := make(chan struct{}, 1)

	go func() {
		factory.waitForSignal()

		signalCh <- struct{}{}
	}()

	select {
	case err := <-errCh:
		return err
	case <-signalCh:
		return nil
	case <-parentCtx.Done():
		return nil
	}
}

func waitForSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	<-sigCh
}

func registerBrokerWithRaft(ctx context.Context, logger *slog.Logger, ctrl controller.MetadataStore, cfg api.BrokerConfig, info api.BrokerInfo) error {
	if cfg.ControllerMode != "raft" {
		return ctrl.RegisterBroker(ctx, info)
	}

	type raftStatus interface {
		ControllerMode() string
		RaftState() string
		RaftLeader() string
	}

	rs, ok := ctrl.(raftStatus)
	if !ok {
		return ctrl.RegisterBroker(ctx, info)
	}

	deadline := time.Now().Add(30 * time.Second)

	var lastErr error

	for time.Now().Before(deadline) {
		state := strings.ToLower(rs.RaftState())
		leaderAddr := rs.RaftLeader()

		switch state {
		case "leader":
			if err := ctrl.RegisterBroker(ctx, info); err != nil {
				lastErr = err
				logger.Warn("register broker via local raft failed", "err", err)
			} else {
				return nil
			}
		default:
			if leaderAddr != "" {
				if err := postRegisterBrokerToLeader(ctx, logger, ctrl, leaderAddr, info); err != nil {
					lastErr = err
					logger.Warn("register broker via leader http failed", "leader", leaderAddr, "err", err)
				} else {
					return nil
				}
			}
		}

		if err := waitWithContext(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("broker registration timed out waiting for raft leader")
	}

	return lastErr
}

func postRegisterBrokerToLeader(
	ctx context.Context,
	logger *slog.Logger,
	ctrl controller.MetadataStore,
	leaderAddr string,
	info api.BrokerInfo,
) error {
	url, err := leaderRegisterBrokerURL(ctx, ctrl, leaderAddr, info)
	if err != nil {
		return err
	}

	payload := struct {
		BrokerID       int    `json:"brokerID"`
		Host           string `json:"host"`
		Port           int    `json:"port,omitempty"`
		HTTPAddr       string `json:"httpAddr,omitempty"`
		ControllerAddr string `json:"controllerAddr,omitempty"`
	}{
		BrokerID:       info.BrokerID,
		Host:           info.Host,
		Port:           info.Port,
		HTTPAddr:       info.HTTPAddr,
		ControllerAddr: info.ControllerAddr,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req) // #nosec G704 -- leader address comes from configured raft peers.
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Warn("failed to close leader register response body", "leader", leaderAddr, "err", closeErr)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("leader register broker http status %d", resp.StatusCode)
	}

	logger.Info("broker registered via raft leader", "leader", leaderAddr, "brokerID", info.BrokerID)

	return nil
}

func leaderRegisterBrokerURL(ctx context.Context, ctrl controller.MetadataStore, leaderAddr string, info api.BrokerInfo) (string, error) {
	if ctrl != nil {
		meta, err := ctrl.GetClusterMetadata(ctx)
		if err == nil {
			if leaderInfo, ok := brokerInfoForLeader(meta.Brokers, leaderAddr); ok {
				if addr, ok := hostPort(leaderInfo.HTTPAddr); ok {
					return "http://" + addr + "/api/controller/brokers", nil
				}
			}
		}
	}

	host, _, err := net.SplitHostPort(leaderAddr)
	if err != nil {
		return "", fmt.Errorf("invalid raft leader address %q: %w", leaderAddr, err)
	}

	port, ok := portFromAddr(info.HTTPAddr)
	if !ok {
		return "", fmt.Errorf("cannot derive http port from advertised http addr %q", info.HTTPAddr)
	}

	return fmt.Sprintf("http://%s:%s/api/controller/brokers", host, port), nil
}

func brokerInfoForLeader(list []api.BrokerInfo, leaderAddr string) (api.BrokerInfo, bool) {
	for _, info := range list {
		if info.ControllerAddr == leaderAddr {
			return info, true
		}
	}

	leaderHost := hostFromAddr(leaderAddr)
	if leaderHost == "" {
		return api.BrokerInfo{}, false
	}

	for _, info := range list {
		for _, candidate := range []string{info.ControllerAddr, info.HTTPAddr, info.Host} {
			if hostFromAddr(candidate) == leaderHost {
				return info, true
			}
		}
	}

	return api.BrokerInfo{}, false
}

func hostPort(addr string) (string, bool) {
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil || u.Host == "" {
			return "", false
		}

		addr = u.Host
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", false
	}

	return net.JoinHostPort(host, port), true
}

func advertisedHTTPAddr(cfg api.BrokerConfig) string {
	port, ok := portFromAddr(cfg.HTTPAddr)
	if !ok {
		return cfg.HTTPAddr
	}

	host := advertisedHost(cfg)
	if host == "" {
		return cfg.HTTPAddr
	}

	return net.JoinHostPort(host, port)
}

func advertisedHost(cfg api.BrokerConfig) string {
	for _, addr := range []string{cfg.AdvertisedAddr, cfg.RaftBindAddr, cfg.BinaryAddr} {
		host := hostFromAddr(addr)
		if host == "" || isWildcardHost(host) {
			continue
		}

		return host
	}

	return ""
}

func hostFromAddr(addr string) string {
	switch {
	case addr == "":
		return ""
	case strings.HasPrefix(addr, ":"):
		return ""
	}

	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}

	return addr
}

func portFromAddr(addr string) (string, bool) {
	switch {
	case addr == "":
		return "", false
	case strings.HasPrefix(addr, ":"):
		port := strings.TrimPrefix(addr, ":")
		if port == "" {
			return "", false
		}

		return port, true
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return "", false
	}

	return port, true
}

func isWildcardHost(host string) bool {
	switch host {
	case "", "0.0.0.0", "::", "[::]", "*":
		return true
	default:
		return false
	}
}

func waitWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func closeWithLog(logger *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		logger.Warn("close failed", "component", name, "err", err)
	}
}
