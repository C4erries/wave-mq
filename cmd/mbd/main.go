package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/httpapi"
	"github.com/c4erries/wave-mq/internal/mqtt"
	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/internal/observability"
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
	}
	if *retentionHours > 0 {
		cfg.RetentionTime = time.Duration(*retentionHours) * time.Hour
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

	offsetStore, err := broker.NewOffsetStore(cfg.DataDir)
	if err != nil {
		logger.Error("offset store init failed", "err", err)
		os.Exit(1)
	}

	b, err := broker.NewBroker(cfg, store, offsetStore)
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

	ready := func() bool { return readyFlag.Load() }
	go func() {
		apiHandler := httpapi.New(b, cfg)
		onStarted := func() { readyFlag.Store(true) }
		if err := observability.StartHTTPServer(ctx, cfg.HTTPAddr, ready, apiHandler.Register, onStarted); err != nil {
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

	waitForSignal()
	cancel()

	_ = netServer.Close()
	_ = mqttServer.Close()
	_ = b.Close()
	_ = store.Close()
	_ = offsetStore.Close()
}

func waitForSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
}
