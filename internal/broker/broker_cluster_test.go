package broker

import (
	"context"
	"testing"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeCluster struct {
	meta api.ClusterMetadata
}

func (f fakeCluster) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx
	return f.meta, nil
}

func (f fakeCluster) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion
	ch := make(chan api.ClusterMetadata, 1)
	ch <- f.meta
	close(ch)
	return ch, nil
}

func (f fakeCluster) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg
	return f.meta, nil
}

func (f fakeCluster) ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark
	return f.meta, nil
}

func (f fakeCluster) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info
	return nil
}

func TestLocalPartitionsSnapshotSingleNode(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "t1", api.TopicConfig{Partitions: 2}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := b.CreateTopic(ctx, "t2", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create: %v", err)
	}
	assignments, err := b.LocalPartitionsSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(assignments) != 3 {
		t.Fatalf("expected 3 assignments, got %d", len(assignments))
	}
	for _, a := range assignments {
		if a.Leader != b.cfg.BrokerID {
			t.Fatalf("unexpected leader %d for %s-%d", a.Leader, a.Topic, a.Partition)
		}
	}
}

func TestLocalPartitionsSnapshotFiltersByLeader(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	offsetStore, _ := NewOffsetStore(dir)
	metaStore, _ := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	cluster := fakeCluster{meta: api.ClusterMetadata{
		ClusterID: "c1",
		Version:   2,
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
		Partitions: []api.PartitionAssignment{
			{Topic: "a", Partition: 0, Leader: 1, Replicas: []int{1}, ISR: []int{1}},
			{Topic: "a", Partition: 1, Leader: 2, Replicas: []int{2}, ISR: []int{2}},
			{Topic: "b", Partition: 0, Leader: 1, Replicas: []int{1}, ISR: []int{1}},
		},
	}}
	b, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore, cluster)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer func() {
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()
	assignments, err := b.LocalPartitionsSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("expected 2 local partitions, got %d", len(assignments))
	}
	for _, a := range assignments {
		if a.Leader != 1 {
			t.Fatalf("unexpected leader %d", a.Leader)
		}
	}
}

func TestClusterMetadataOverridesLocalCache(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	offsetStore, _ := NewOffsetStore(dir)
	metaStore, _ := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	ctx := context.Background()
	if err := metaStore.AppendCreateTopic(ctx, metadata.CreateTopicEvent{
		Name:              "stale",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Partitions: []metadata.PartitionSpec{{
			ID:       0,
			Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader}},
		}},
	}); err != nil {
		t.Fatalf("append topic: %v", err)
	}
	cluster := fakeCluster{meta: api.ClusterMetadata{
		ClusterID: "c1",
		Version:   1,
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
		Partitions: []api.PartitionAssignment{{
			Topic:       "fresh",
			Partition:   0,
			Replicas:    []int{2, 1},
			ISR:         []int{2, 1},
			Leader:      2,
			LeaderEpoch: 3,
		}},
	}}
	b, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore, cluster)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer func() {
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.topics) != 1 {
		t.Fatalf("expected only cluster topic, got %d", len(b.topics))
	}
	topic := b.topics["fresh"]
	if topic == nil {
		t.Fatalf("expected fresh topic to be loaded")
	}
	part := topic.Partitions[0]
	if part == nil {
		t.Fatalf("expected partition 0 for topic fresh")
	}
	if part.Metadata.Replica.Role != api.RoleFollower {
		t.Fatalf("expected follower role, got %v", part.Metadata.Replica.Role)
	}
	if part.Metadata.Leader != 2 {
		t.Fatalf("expected leader 2, got %d", part.Metadata.Leader)
	}
}
