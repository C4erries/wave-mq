package broker

import (
	"context"
	"testing"

	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func newTestBroker(t *testing.T) (*Broker, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	b, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	cleanup := func() {
		_ = b.Close()
		_ = store.Close()
	}
	return b, cleanup
}

func TestCreateTopicAndProduceFetch(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "test", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	records := []api.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
		{Value: []byte("three")},
	}
	base, err := b.Produce(ctx, "test", 0, records)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if base != 0 {
		t.Fatalf("expected base offset 0, got %d", base)
	}
	got, err := b.Fetch(ctx, "test", 0, 0, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(got))
	}
	for i, r := range got {
		if r.Offset != api.Offset(i) {
			t.Fatalf("offset mismatch at %d: %d", i, r.Offset)
		}
		if string(r.Value) != string(records[i].Value) {
			t.Fatalf("value mismatch at %d: %s", i, string(r.Value))
		}
	}
}

func TestListOffsets(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "test", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	records := []api.Record{{Value: []byte("a")}, {Value: []byte("b")}}
	if _, err := b.Produce(ctx, "test", 0, records); err != nil {
		t.Fatalf("produce: %v", err)
	}
	earliest, latest, err := b.ListOffsets(ctx, "test", 0)
	if err != nil {
		t.Fatalf("list offsets: %v", err)
	}
	if earliest != 0 {
		t.Fatalf("earliest expected 0, got %d", earliest)
	}
	if latest != 1 {
		t.Fatalf("latest expected 1, got %d", latest)
	}
}

func TestMetadata(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "alpha", api.TopicConfig{Partitions: 2}); err != nil {
		t.Fatalf("create alpha: %v", err)
	}
	if err := b.CreateTopic(ctx, "beta", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create beta: %v", err)
	}
	meta, err := b.Metadata(ctx, nil)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if len(meta) != 3 {
		t.Fatalf("expected metadata for 3 partitions, got %d", len(meta))
	}
	for _, m := range meta {
		if m.Replica.BrokerID != 1 || m.Replica.Role != api.RoleLeader {
			t.Fatalf("unexpected replica info: %+v", m.Replica)
		}
	}
	filtered, err := b.Metadata(ctx, []string{"beta"})
	if err != nil {
		t.Fatalf("metadata filtered: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 partition for beta, got %d", len(filtered))
	}
	if filtered[0].Replica.Topic != "beta" {
		t.Fatalf("unexpected topic in filtered metadata: %s", filtered[0].Replica.Topic)
	}
}

func TestCommitOffsetMonotonic(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "t", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := b.CommitOffset(ctx, "g1", "t", 0, 2); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if err := b.CommitOffset(ctx, "g1", "t", 0, 5); err != nil {
		t.Fatalf("commit 5: %v", err)
	}
	off, err := b.FetchCommitted(ctx, "g1", "t", 0)
	if err != nil {
		t.Fatalf("fetch committed: %v", err)
	}
	if off != 5 {
		t.Fatalf("expected offset 5, got %d", off)
	}
	if err := b.CommitOffset(ctx, "g1", "t", 0, 4); err == nil {
		t.Fatalf("expected regression error, got nil")
	}
	off, _ = b.FetchCommitted(ctx, "g1", "t", 0)
	if off != 5 {
		t.Fatalf("offset regressed to %d", off)
	}
}

func TestJoinGroupAssignments(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "t", api.TopicConfig{Partitions: 4}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	assign1, err := b.JoinGroup(ctx, "g1", "m1", []string{"t"})
	if err != nil {
		t.Fatalf("join m1: %v", err)
	}
	assign2, err := b.JoinGroup(ctx, "g1", "m2", []string{"t"})
	if err != nil {
		t.Fatalf("join m2: %v", err)
	}
	cover := make(map[int]string)
	for _, p := range assign1["t"] {
		cover[p] = "m1"
	}
	for _, p := range assign2["t"] {
		if owner, ok := cover[p]; ok {
			t.Fatalf("partition %d assigned to both %s and m2", p, owner)
		}
		cover[p] = "m2"
	}
	if len(cover) != 4 {
		t.Fatalf("not all partitions assigned: %v", cover)
	}
}

func TestLeaveGroupReassignment(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "t", api.TopicConfig{Partitions: 3}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	_, err := b.JoinGroup(ctx, "g", "m1", []string{"t"})
	if err != nil {
		t.Fatalf("join m1: %v", err)
	}
	_, err = b.JoinGroup(ctx, "g", "m2", []string{"t"})
	if err != nil {
		t.Fatalf("join m2: %v", err)
	}
	if err := b.LeaveGroup(ctx, "g", "m1"); err != nil {
		t.Fatalf("leave m1: %v", err)
	}
	assignments, err := b.JoinGroup(ctx, "g", "m2", []string{"t"})
	if err != nil {
		t.Fatalf("rejoin m2: %v", err)
	}
	if len(assignments["t"]) != 3 {
		t.Fatalf("expected m2 to own all partitions after m1 left, got %v", assignments["t"])
	}
}
