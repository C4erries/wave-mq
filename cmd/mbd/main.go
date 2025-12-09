package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	var readyFlag atomic.Bool
	var (
		dataDir           = flag.String("data-dir", "data", "path to broker data directory")
		binaryAddr        = flag.String("bind", ":7912", "address for binary protocol listener")
		mqttAddr          = flag.String("mqtt", ":1883", "address for MQTT listener")
		httpAddr          = flag.String("http", ":8090", "address for metrics/health HTTP listener")
		brokerID          = flag.Int("broker-id", 1, "numeric broker id")
		replicationFactor = flag.Int("replication-factor", 1, "default replication factor for new topics")
		segmentBytes      = flag.Int64("segment-bytes", 64<<20, "max segment size before rotation")
		retentionBytes    = flag.Int64("retention-bytes", -1, "total retention budget in bytes (-1 for unlimited)")
		retentionHours    = flag.Int("retention-hours", 0, "retention by age in hours (0 disables time-based retention)")
		controllerMode    = flag.String("controller", "single", "controller mode: single or raft")
		raftDir           = flag.String("raft-dir", "", "directory for Raft state (empty = in-memory)")
		raftBind          = flag.String("raft-bind", "", "raft bind address for controller (host:port)")
		raftPeers         = flag.String("raft-peer", "", "comma-separated list of raft peer addresses")
		enableReplication = flag.Bool("replication", false, "enable follower replication (experimental)")
	)
	flag.Parse()

	cfg := api.BrokerConfig{
		BrokerID:          *brokerID,
		ReplicationFactor: *replicationFactor,
		DataDir:           *dataDir,
		BinaryAddr:        *binaryAddr,
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
	if cfg.ControllerMode == "raft" {
		if cfg.RaftBindAddr == "" {
			logger.Error("raft controller mode requires -raft-bind")
			os.Exit(1)
		}
		if len(cfg.RaftPeers) == 0 {
			logger.Error("raft controller mode requires at least one -raft-peer (including self)")
			os.Exit(1)
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
	}

	store, err := storage.NewManager(storage.Config{
		DataDir:         cfg.DataDir,
		MaxSegmentBytes: cfg.MaxSegmentBytes,
		IndexInterval:   1024,
		SegmentMaxAge:   cfg.RetentionTime,
		MaxLogBytes:     cfg.RetentionBytes,
		// TODO: load index interval/segment age from config or flags.
	})
	if err != nil {
		logger.Error("storage init failed", "err", err)
		os.Exit(1)
	}

	if err := store.Recover(context.Background()); err != nil {
		logger.Error("storage recover failed", "err", err)
		os.Exit(1)
	}

	metadataStore, err := metadata.NewStore(cfg)
	if err != nil {
		logger.Error("metadata store init failed", "err", err)
		os.Exit(1)
	}
	recoveredTopics, err := metadataStore.RecoverTopics(context.Background())
	if err != nil {
		logger.Error("metadata recover failed", "err", err)
		os.Exit(1)
	}
	ctrl, err := controller.NewController(cfg, recoveredTopics.Topics)
	if err != nil {
		logger.Error("controller init failed", "err", err)
		os.Exit(1)
	}
	logger.Info("controller initialized", "mode", cfg.ControllerMode)

	brokerHost := cfg.AdvertisedAddr
	if brokerHost == "" {
		brokerHost = cfg.BinaryAddr
	}
	bInfo := api.BrokerInfo{
		BrokerID: cfg.BrokerID,
		Host:     brokerHost,
	}
	if err := registerBrokerWithRaft(context.Background(), logger, ctrl, cfg, bInfo); err != nil {
		logger.Error("broker registration failed", "err", err)
		os.Exit(1)
	}
	logger.Info("broker registered in controller", "brokerID", bInfo.BrokerID, "host", bInfo.Host)

	metaSnapshot, err := ctrl.GetClusterMetadata(context.Background())
	if err != nil {
		logger.Error("cluster metadata fetch failed", "err", err)
		os.Exit(1)
	}
	logger.Info("fetched initial cluster metadata", "version", metaSnapshot.Version, "partitions", len(metaSnapshot.Partitions))

	offsetStore, err := broker.NewOffsetStore(cfg.DataDir)
	if err != nil {
		logger.Error("offset store init failed", "err", err)
		os.Exit(1)
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metadataStore, ctrl, &metaSnapshot)
	if err != nil {
		logger.Error("broker init failed", "err", err)
		os.Exit(1)
	}

	netServer, err := netproto.NewServer(cfg.BinaryAddr, b)
	if err != nil {
		logger.Error("netproto init failed", "err", err)
		os.Exit(1)
	}
	mqttServer, err := mqtt.NewServer(cfg.MQTTAddr, b)
	if err != nil {
		logger.Error("mqtt init failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.StartClusterMetadataWatcher(ctx); err != nil {
		logger.Error("cluster metadata watcher init failed", "err", err)
		os.Exit(1)
	}
	if cfg.Replication {
		rep := replication.NewBinaryReplicator()
		replMgr := replication.NewManager(cfg, store, ctrl, rep)
		go func() {
			logger.Info("replication manager starting", "mode", cfg.ControllerMode)
			if err := replMgr.Run(ctx); err != nil && err != context.Canceled {
				logger.Error("replication manager stopped", "err", err)
				cancel()
			}
		}()
	}

	ready := func() bool { return readyFlag.Load() }
	go func() {
		apiHandler := httpapi.New(b, cfg, ctrl)
		if err := observability.StartHTTPServer(ctx, cfg.HTTPAddr, ready, apiHandler.Register, nil); err != nil {
			readyFlag.Store(false)
			logger.Error("http server stopped", "err", err)
			cancel()
		}
	}()

	// Start servers.
	go func() {
		if err := netServer.ListenAndServe(ctx); err != nil {
			readyFlag.Store(false)
			logger.Error("binary server stopped", "err", err)
			cancel()
		}
	}()
	go func() {
		if err := mqttServer.ListenAndServe(ctx); err != nil {
			readyFlag.Store(false)
			logger.Error("mqtt server stopped", "err", err)
			cancel()
		}
	}()
	readyFlag.Store(true)

	waitForSignal()
	cancel()

	_ = netServer.Close()
	_ = mqttServer.Close()
	_ = b.Close()
	_ = store.Close()
	_ = offsetStore.Close()
	_ = metadataStore.Close()
}

func waitForSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
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
				if err := postRegisterBrokerToLeader(ctx, logger, leaderAddr, cfg.HTTPAddr, info); err != nil {
					lastErr = err
					logger.Warn("register broker via leader http failed", "leader", leaderAddr, "err", err)
				} else {
					return nil
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("broker registration timed out waiting for raft leader")
	}
	return lastErr
}

func postRegisterBrokerToLeader(ctx context.Context, logger *slog.Logger, leaderAddr, localHTTP string, info api.BrokerInfo) error {
	host, _, err := net.SplitHostPort(leaderAddr)
	if err != nil {
		return fmt.Errorf("invalid raft leader address %q: %w", leaderAddr, err)
	}
	port := ""
	if strings.HasPrefix(localHTTP, ":") {
		port = localHTTP[1:]
	} else {
		if h, p, err := net.SplitHostPort(localHTTP); err == nil {
			_ = h
			port = p
		}
	}
	if port == "" {
		return fmt.Errorf("cannot derive http port from %q", localHTTP)
	}
	url := fmt.Sprintf("http://%s:%s/api/controller/brokers", host, port)
	payload := struct {
		BrokerID int    `json:"brokerID"`
		Host     string `json:"host"`
		Port     int    `json:"port,omitempty"`
	}{
		BrokerID: info.BrokerID,
		Host:     info.Host,
		Port:     info.Port,
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("leader register broker http status %d", resp.StatusCode)
	}
	logger.Info("broker registered via raft leader", "leader", host, "brokerID", info.BrokerID)
	return nil
}
