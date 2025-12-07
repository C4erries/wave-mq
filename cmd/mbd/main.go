package main

import (
	"context"
	"flag"
	"log/slog"
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
	if err := ctrl.RegisterBroker(context.Background(), bInfo); err != nil {
		logger.Error("broker registration failed", "err", err)
		os.Exit(1)
	}
	logger.Info("broker registered in controller", "brokerID", bInfo.BrokerID, "host", bInfo.Host)

	offsetStore, err := broker.NewOffsetStore(cfg.DataDir)
	if err != nil {
		logger.Error("offset store init failed", "err", err)
		os.Exit(1)
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metadataStore, ctrl)
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
