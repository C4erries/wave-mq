package replication

import (
	"context"
	"testing"

	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestWALSinkApplyBatch(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	}()

	sink := NewWALSink(store, "alpha", 0)
	records := []api.Record{
		{Value: []byte("a")},
		{Value: []byte("b")},
	}

	last, err := sink.ApplyBatch(context.Background(), records, -1)
	if err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	if last != 1 {
		t.Fatalf("expected last offset 1, got %d", last)
	}

	log, err := store.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	got, err := log.Read(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(got) != 2 || string(got[0].Value) != "a" || string(got[1].Value) != "b" {
		t.Fatalf("unexpected records: %+v", got)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}
}

func TestWALSinkMaintainsOffsetsWhenLeaderBehind(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	}()

	sink := NewWALSink(store, "alpha", 0)

	records := []api.Record{{Value: []byte("one")}, {Value: []byte("two")}, {Value: []byte("three")}}
	if _, err := sink.ApplyBatch(context.Background(), records, 5); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	next, err := sink.(HighWatermarkAligner).EnsureLeaderHighWatermark(context.Background(), 1)
	if err != nil {
		t.Fatalf("ensure leader hwm: %v", err)
	}

	if next != 3 {
		t.Fatalf("expected next offset 3 after alignment, got %d", next)
	}

	log, err := store.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	read, err := log.Read(context.Background(), 0, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(read) != 3 || string(read[2].Value) != "three" {
		t.Fatalf("unexpected records after alignment: %+v", read)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}
}
