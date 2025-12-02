package metadata

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestStoreRecoverCreateTopicEvents(t *testing.T) {
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
	store.Close()

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
	store.Close()

	// Corrupt tail with partial bytes.
	path := filepath.Join(dir, "metadata.log")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := f.Write([]byte{0xAA, 0xBB, 0xCC}); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	f.Close()

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
	store.Close()

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
	store.Close()

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
	store.Close()
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
