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

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

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

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Corrupt tail
	path := store.path

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := f.WriteString("junk"); err != nil {
		t.Fatalf("write junk: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close corrupting file handle: %v", err)
	}

	store, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	offsets, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	if offsets["g"]["t"][0] != 1 {
		t.Fatalf("expected offset 1 after recovery, got %d", offsets["g"]["t"][0])
	}
}

func TestOffsetStoreCompact(t *testing.T) {
	dir := t.TempDir()

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx := context.Background()
	if err := store.AppendCommit(ctx, "g1", "t1", 0, 1); err != nil {
		t.Fatalf("append g1/t1#1: %v", err)
	}

	if err := store.AppendCommit(ctx, "g1", "t1", 0, 2); err != nil {
		t.Fatalf("append g1/t1#2: %v", err)
	}

	if err := store.AppendCommit(ctx, "g2", "t2", 1, 3); err != nil {
		t.Fatalf("append g2/t2: %v", err)
	}

	offsets := map[string]map[string]map[int]api.Offset{
		"g1": {"t1": {0: 2}},
		"g2": {"t2": {1: 3}},
	}
	if err := store.Compact(ctx, offsets); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	got, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("recover after compact: %v", err)
	}

	if got["g1"]["t1"][0] != 2 || got["g2"]["t2"][1] != 3 {
		t.Fatalf("unexpected offsets after compact: %#v", got)
	}
}

func TestOffsetStoreRecoverKeepsWriterAtEnd(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if err := store.AppendCommit(ctx, "g1", "t1", 0, 11); err != nil {
		t.Fatalf("append g1/t1: %v", err)
	}

	if err := store.AppendCommit(ctx, "g2", "t2", 1, 22); err != nil {
		t.Fatalf("append g2/t2: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store before reopen: %v", err)
	}

	store, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	if _, err := store.Recover(ctx); err != nil {
		t.Fatalf("recover store: %v", err)
	}

	if err := store.AppendCommit(ctx, "g3", "t3", 2, 33); err != nil {
		t.Fatalf("append g3/t3: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store after append: %v", err)
	}

	store, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("reopen store second time: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	offsets, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("recover second time: %v", err)
	}

	if offsets["g1"]["t1"][0] != 11 {
		t.Fatalf("expected g1/t1 offset 11, got %d", offsets["g1"]["t1"][0])
	}

	if offsets["g2"]["t2"][1] != 22 {
		t.Fatalf("expected g2/t2 offset 22, got %d", offsets["g2"]["t2"][1])
	}

	if offsets["g3"]["t3"][2] != 33 {
		t.Fatalf("expected g3/t3 offset 33, got %d", offsets["g3"]["t3"][2])
	}
}

func TestOffsetStoreAppendAfterCompactKeepsRecords(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	if err := store.AppendCommit(ctx, "g1", "t1", 0, 7); err != nil {
		t.Fatalf("append g1/t1: %v", err)
	}

	if err := store.AppendCommit(ctx, "g2", "t2", 1, 8); err != nil {
		t.Fatalf("append g2/t2: %v", err)
	}

	snapshot := map[string]map[string]map[int]api.Offset{
		"g1": {"t1": {0: 7}},
		"g2": {"t2": {1: 8}},
	}
	if err := store.Compact(ctx, snapshot); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if err := store.AppendCommit(ctx, "g3", "t3", 2, 9); err != nil {
		t.Fatalf("append g3/t3 after compact: %v", err)
	}

	offsets, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	if offsets["g1"]["t1"][0] != 7 {
		t.Fatalf("expected g1/t1 offset 7, got %d", offsets["g1"]["t1"][0])
	}

	if offsets["g2"]["t2"][1] != 8 {
		t.Fatalf("expected g2/t2 offset 8, got %d", offsets["g2"]["t2"][1])
	}

	if offsets["g3"]["t3"][2] != 9 {
		t.Fatalf("expected g3/t3 offset 9, got %d", offsets["g3"]["t3"][2])
	}
}
