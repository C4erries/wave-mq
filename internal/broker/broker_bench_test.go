package broker

import (
	"context"
	"testing"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func BenchmarkBrokerProduce(b *testing.B) {
	dir := b.TempDir()
	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 64 << 20,
		IndexInterval:   16,
		SyncOnAppend:    false, // throughput oriented
	})
	if err != nil {
		b.Fatalf("storage: %v", err)
	}
	offsetStore, err := NewOffsetStore(dir)
	if err != nil {
		b.Fatalf("offset store: %v", err)
	}
	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		b.Fatalf("metadata store: %v", err)
	}
	br, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore)
	if err != nil {
		b.Fatalf("broker: %v", err)
	}
	defer func() {
		br.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()
	ctx := context.Background()
	if err := br.CreateTopic(ctx, "bench", api.TopicConfig{Partitions: 1}); err != nil {
		b.Fatalf("create topic: %v", err)
	}
	rec := api.Record{Value: make([]byte, 100)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := br.Produce(ctx, "bench", 0, []api.Record{rec}); err != nil {
			b.Fatalf("produce: %v", err)
		}
	}
}

func BenchmarkBrokerFetch(b *testing.B) {
	dir := b.TempDir()
	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 64 << 20,
		IndexInterval:   16,
		SyncOnAppend:    false,
	})
	if err != nil {
		b.Fatalf("storage: %v", err)
	}
	offsetStore, err := NewOffsetStore(dir)
	if err != nil {
		b.Fatalf("offset store: %v", err)
	}
	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		b.Fatalf("metadata store: %v", err)
	}
	br, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore)
	if err != nil {
		b.Fatalf("broker: %v", err)
	}
	defer func() {
		br.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()
	ctx := context.Background()
	if err := br.CreateTopic(ctx, "bench", api.TopicConfig{Partitions: 1}); err != nil {
		b.Fatalf("create topic: %v", err)
	}
	rec := api.Record{Value: make([]byte, 200)}
	for i := 0; i < 10000; i++ {
		if _, err := br.Produce(ctx, "bench", 0, []api.Record{rec}); err != nil {
			b.Fatalf("produce preload: %v", err)
		}
	}
	b.ResetTimer()
	offset := api.Offset(0)
	for i := 0; i < b.N; i++ {
		if _, err := br.Fetch(ctx, "bench", 0, offset, 4096); err != nil {
			b.Fatalf("fetch: %v", err)
		}
		offset += 10
		if offset > 9000 {
			offset = 0
		}
	}
}
