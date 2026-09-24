package metadata

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestStoreRecoverCreateTopicEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := api.BrokerConfig{DataDir: dir}

	store, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx := context.Background()
	ev1 := CreateTopicEvent{
		Name:              "alpha",
		NumPartitions:     2,
		ReplicationFactor: 1,
		Partitions: []PartitionSpec{
			{ID: 0, Replicas: []ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1}}},
			{ID: 1, Replicas: []ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1}}},
		},
	}
	ev2 := CreateTopicEvent{
		Name:              "beta",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Partitions: []PartitionSpec{
			{ID: 0, Replicas: []ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 2}}},
		},
	}

	if err := store.AppendCreateTopic(ctx, ev1); err != nil {
		t.Fatalf("append alpha: %v", err)
	}

	if err := store.AppendCreateTopic(ctx, ev2); err != nil {
		t.Fatalf("append beta: %v", err)
	}
	// duplicate should be ignored during recovery
	if err := store.AppendCreateTopic(ctx, ev1); err != nil {
		t.Fatalf("append alpha dup: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store before reopen: %v", err)
	}

	store, err = NewStore(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	recovered, err := store.RecoverTopics(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	if len(recovered.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(recovered.Topics))
	}

	alpha := recovered.Topics["alpha"]
	if alpha.ReplicationFactor != 1 || alpha.NumPartitions != 2 {
		t.Fatalf("unexpected alpha metadata: %+v", alpha)
	}

	if len(alpha.Partitions) != 2 {
		t.Fatalf("expected 2 partitions for alpha, got %d", len(alpha.Partitions))
	}

	if alpha.Partitions[0].Replicas[0].LeaderEpoch != 1 {
		t.Fatalf("expected leader epoch 1, got %d", alpha.Partitions[0].Replicas[0].LeaderEpoch)
	}

	if _, ok := recovered.Topics["beta"]; !ok {
		t.Fatalf("missing beta in recovered topics")
	}
}

func TestStoreTruncatesPartialTail(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := api.BrokerConfig{DataDir: dir}

	store, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx := context.Background()

	ev := CreateTopicEvent{
		Name:              "gamma",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Partitions: []PartitionSpec{
			{ID: 0, Replicas: []ReplicaSpec{{BrokerID: 42, Role: api.RoleLeader, LeaderEpoch: 3}}},
		},
	}
	if err := store.AppendCreateTopic(ctx, ev); err != nil {
		t.Fatalf("append: %v", err)
	}

	info, err := store.f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	sizeBefore := info.Size()

	if err := store.Close(); err != nil {
		t.Fatalf("close store before corruption: %v", err)
	}

	// Corrupt tail with partial bytes.
	path := filepath.Join(dir, "metadata.log")

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}

	if _, err := f.Write([]byte{0xAA, 0xBB, 0xCC}); err != nil {
		t.Fatalf("write junk: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close corruption file: %v", err)
	}

	store, err = NewStore(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	recovered, err := store.RecoverTopics(ctx)
	if err != nil {
		t.Fatalf("recover after corruption: %v", err)
	}

	if len(recovered.Topics) != 1 {
		t.Fatalf("expected 1 topic after recovery, got %d", len(recovered.Topics))
	}

	info, err = store.f.Stat()
	if err != nil {
		t.Fatalf("stat after recover: %v", err)
	}

	if info.Size() != sizeBefore {
		t.Fatalf("expected truncated size %d, got %d", sizeBefore, info.Size())
	}
}

func TestAppendAfterRecoveryAppendsToEnd(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := api.BrokerConfig{DataDir: dir}
	ctx := context.Background()

	store, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ev1 := CreateTopicEvent{
		Name:              "first",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Partitions: []PartitionSpec{
			{ID: 0, Replicas: []ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 0}}},
		},
	}
	ev2 := CreateTopicEvent{
		Name:              "second",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Partitions: []PartitionSpec{
			{ID: 0, Replicas: []ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 0}}},
		},
	}

	if err := store.AppendCreateTopic(ctx, ev1); err != nil {
		t.Fatalf("append ev1: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store after first append: %v", err)
	}

	// Simulate broker restart: recover then append new topic.
	store, err = NewStore(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	if _, err := store.RecoverTopics(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if err := store.AppendCreateTopic(ctx, ev2); err != nil {
		t.Fatalf("append ev2 after recover: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store after second append: %v", err)
	}

	// Final reopen and recover should see both topics intact.
	store, err = NewStore(cfg)
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	defer store.Close()

	recovered, err := store.RecoverTopics(ctx)
	if err != nil {
		t.Fatalf("final recover: %v", err)
	}

	if len(recovered.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(recovered.Topics))
	}

	if _, ok := recovered.Topics["first"]; !ok {
		t.Fatalf("missing first topic after append")
	}

	if _, ok := recovered.Topics["second"]; !ok {
		t.Fatalf("missing second topic after append")
	}
}

func TestStoreRecoverReplicationFactorGreaterThanOne(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := api.BrokerConfig{DataDir: dir}

	store, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx := context.Background()

	ev := CreateTopicEvent{
		Name:              "rf3",
		NumPartitions:     1,
		ReplicationFactor: 3,
		Partitions: []PartitionSpec{
			{ID: 0, Replicas: []ReplicaSpec{
				{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 2},
				{BrokerID: 2, Role: api.RoleFollower, LeaderEpoch: 2},
				{BrokerID: 3, Role: api.RoleFollower, LeaderEpoch: 2},
			}},
		},
	}
	if err := store.AppendCreateTopic(ctx, ev); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store before reopen: %v", err)
	}

	store, err = NewStore(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	recovered, err := store.RecoverTopics(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	topic, ok := recovered.Topics["rf3"]
	if !ok {
		t.Fatalf("topic not recovered")
	}

	if topic.ReplicationFactor != 3 {
		t.Fatalf("expected rf=3, got %d", topic.ReplicationFactor)
	}

	if len(topic.Partitions) != 1 || len(topic.Partitions[0].Replicas) != 3 {
		t.Fatalf("expected 1 partition with 3 replicas, got %+v", topic.Partitions)
	}
}

func TestEncodeCreateTopicEventRejectsInvalidReplicaRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		role api.PartitionRole
	}{
		{name: "negative", role: api.PartitionRole(-1)},
		{name: "unknown positive", role: api.PartitionRole(99)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ev := CreateTopicEvent{
				Name:              "bad-role",
				NumPartitions:     1,
				ReplicationFactor: 1,
				Partitions: []PartitionSpec{
					{ID: 0, Replicas: []ReplicaSpec{{BrokerID: 1, Role: tc.role, LeaderEpoch: 1}}},
				},
			}

			_, err := encodeCreateTopicEvent(ev)
			if err == nil {
				t.Fatalf("expected error for invalid role %d", tc.role)
			}

			if !strings.Contains(err.Error(), "invalid partition role") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestDecodeRecordRejectsInvalidReplicaRole(t *testing.T) {
	t.Parallel()

	ev := CreateTopicEvent{
		Name:              "role-mutation",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Partitions: []PartitionSpec{
			{ID: 0, Replicas: []ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1}}},
		},
	}

	payload, err := encodeCreateTopicEvent(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	roleOffset := 2 + // version + type
		2 + len(ev.Name) + // name
		2 + // partitions
		2 + // rf
		8 + // retention bytes
		8 + // retention time
		2 + // partition spec count
		4 + // partition id
		2 + // replica count
		4 // broker id
	payload[roleOffset] = byte(255)

	record, err := wrapRecord(payload)
	if err != nil {
		t.Fatalf("wrap record: %v", err)
	}

	_, err = decodeRecord(record[4:])
	if err == nil {
		t.Fatal("expected decode error for invalid role")
	}

	if !strings.Contains(err.Error(), "invalid partition role") {
		t.Fatalf("unexpected decode error: %v", err)
	}
}
