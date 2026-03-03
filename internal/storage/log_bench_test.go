package storage

import (
	"context"
	"testing"

	"github.com/c4erries/wave-mq/pkg/api"
)

func BenchmarkLogAppendSmallRecords(b *testing.B) {
	dir := b.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 64 << 20,
		IndexInterval:   16,
		SyncOnAppend:    false, // benchmark throughput
	})
	if err != nil {
		b.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "bench", Partition: 0})
	if err != nil {
		b.Fatalf("open log: %v", err)
	}
	b.Cleanup(func() {
		if err := log.Close(); err != nil {
			b.Errorf("close log: %v", err)
		}

		if err := m.Close(); err != nil {
			b.Errorf("close manager: %v", err)
		}
	})

	ctx := context.Background()
	payload := make([]byte, 100)

	records := make([]api.Record, 10)
	for i := range records {
		records[i].Value = payload
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.AppendBatch(ctx, records); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func BenchmarkLogReadSequential(b *testing.B) {
	dir := b.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 64 << 20,
		IndexInterval:   16,
		SyncOnAppend:    false,
	})
	if err != nil {
		b.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "bench", Partition: 0})
	if err != nil {
		b.Fatalf("open log: %v", err)
	}
	b.Cleanup(func() {
		if err := log.Close(); err != nil {
			b.Errorf("close log: %v", err)
		}

		if err := m.Close(); err != nil {
			b.Errorf("close manager: %v", err)
		}
	})

	ctx := context.Background()

	payload := make([]byte, 128)
	for i := 0; i < 100000; i++ {
		if _, err := log.Append(ctx, api.Record{Value: payload}); err != nil {
			b.Fatalf("append: %v", err)
		}
	}

	b.ResetTimer()

	offset := api.Offset(0)
	for i := 0; i < b.N; i++ {
		if _, err := log.Read(ctx, offset, 4096); err != nil {
			b.Fatalf("read: %v", err)
		}

		offset += 10
		if offset > 90000 {
			offset = 0
		}
	}
}

func BenchmarkLogRandomRead(b *testing.B) {
	dir := b.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 64 << 20,
		IndexInterval:   16,
		SyncOnAppend:    false,
	})
	if err != nil {
		b.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "bench", Partition: 0})
	if err != nil {
		b.Fatalf("open log: %v", err)
	}
	b.Cleanup(func() {
		if err := log.Close(); err != nil {
			b.Errorf("close log: %v", err)
		}

		if err := m.Close(); err != nil {
			b.Errorf("close manager: %v", err)
		}
	})

	ctx := context.Background()

	payload := make([]byte, 128)
	for i := 0; i < 50000; i++ {
		if _, err := log.Append(ctx, api.Record{Value: payload}); err != nil {
			b.Fatalf("append: %v", err)
		}
	}

	b.ResetTimer()

	offset := api.Offset(0)
	for i := 0; i < b.N; i++ {
		offset = api.Offset((i * 37) % 50000)
		if _, err := log.Read(ctx, offset, 2048); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}
