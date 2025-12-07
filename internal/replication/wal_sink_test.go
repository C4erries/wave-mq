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
	log.Close()
}
