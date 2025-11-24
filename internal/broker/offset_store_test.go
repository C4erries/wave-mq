package broker

import (
	"context"
	"os"
	"testing"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestOffsetStoreRecover(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	commits := []struct {
		group     string
		topic     string
		partition int
		offset    api.Offset
	}{
		{"g1", "t1", 0, 1},
		{"g1", "t1", 0, 2},
		{"g1", "t1", 1, 5},
		{"g2", "t1", 0, 3},
	}
	for _, c := range commits {
		if err := store.AppendCommit(ctx, c.group, c.topic, c.partition, c.offset); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	store.Close()

	store, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()
	offsets, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if offsets["g1"]["t1"][0] != 2 {
		t.Fatalf("expected last offset 2, got %d", offsets["g1"]["t1"][0])
	}
	if offsets["g1"]["t1"][1] != 5 {
		t.Fatalf("expected offset 5 for partition 1")
	}
	if offsets["g2"]["t1"][0] != 3 {
		t.Fatalf("expected offset 3 for g2")
	}
}

func TestOffsetStoreRecoverTruncatesTail(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	if err := store.AppendCommit(ctx, "g", "t", 0, 1); err != nil {
		t.Fatalf("append: %v", err)
	}
	store.Close()

	// Corrupt tail
	path := store.path
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Write([]byte("junk")); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	f.Close()

	store, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	offsets, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if offsets["g"]["t"][0] != 1 {
		t.Fatalf("expected offset 1 after recovery, got %d", offsets["g"]["t"][0])
	}
}
