package broker

import (
	"context"
	"testing"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type partitionAwareCluster struct {
	meta api.ClusterMetadata
}

func (p *partitionAwareCluster) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx
	return p.meta, nil
}

func (p *partitionAwareCluster) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata, 1)
	ch <- p.meta

	close(ch)

	return ch, nil
}

func (p *partitionAwareCluster) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return p.meta, nil
}

func (p *partitionAwareCluster) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return p.meta, nil
}

func (p *partitionAwareCluster) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func TestBrokerFiltersNonLocalPartitionsOnBootstrap(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	offsetStore, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	ctx := context.Background()

	ev := metadata.CreateTopicEvent{
		Name:              "alpha",
		NumPartitions:     2,
		ReplicationFactor: 1,
		Partitions: []metadata.PartitionSpec{
			{ID: 0, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 0}}},
			{ID: 1, Replicas: []metadata.ReplicaSpec{{BrokerID: 2, Role: api.RoleLeader, LeaderEpoch: 0}}},
		},
	}
	if err := metaStore.AppendCreateTopic(ctx, ev); err != nil {
		t.Fatalf("append create topic: %v", err)
	}

	cluster := &partitionAwareCluster{
		meta: api.ClusterMetadata{
			Brokers: []api.BrokerInfo{{BrokerID: 1}, {BrokerID: 2}},
			Partitions: []api.PartitionAssignment{
				{Topic: "alpha", Partition: 0, Replicas: []int{1}, ISR: []int{1}, Leader: 1},
				{Topic: "alpha", Partition: 1, Replicas: []int{2}, ISR: []int{2}, Leader: 2},
			},
		},
	}

	b, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore, cluster, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	defer func() {
		_ = b.Close()
		_ = store.Close()
		_ = offsetStore.Close()
		_ = metaStore.Close()
	}()

	b.mu.RLock()
	topic := b.topics["alpha"]
	b.mu.RUnlock()

	if topic == nil {
		t.Fatalf("topic alpha not loaded")
	}

	if len(topic.Partitions) != 1 {
		t.Fatalf("expected only 1 local partition, got %d", len(topic.Partitions))
	}

	if _, ok := topic.Partitions[0]; !ok {
		t.Fatalf("expected partition 0 to be loaded")
	}

	if _, ok := topic.Partitions[1]; ok {
		t.Fatalf("partition 1 should have been skipped")
	}

	assignments, err := b.LocalPartitionsSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if len(assignments) != 1 || assignments[0].Partition != 0 || assignments[0].Leader != 1 {
		t.Fatalf("unexpected assignments: %+v", assignments)
	}
}

func TestBrokerLoadsLeaderAndFollowerReplicas(t *testing.T) {
	ctx := context.Background()
	ev := metadata.CreateTopicEvent{
		Name:              "alpha",
		NumPartitions:     2,
		ReplicationFactor: 2,
		Partitions: []metadata.PartitionSpec{
			{ID: 0, Replicas: []metadata.ReplicaSpec{
				{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 0},
				{BrokerID: 2, Role: api.RoleFollower, LeaderEpoch: 0},
			}},
			{ID: 1, Replicas: []metadata.ReplicaSpec{
				{BrokerID: 2, Role: api.RoleLeader, LeaderEpoch: 0},
				{BrokerID: 1, Role: api.RoleFollower, LeaderEpoch: 0},
			}},
		},
	}
	meta := api.ClusterMetadata{
		Brokers: []api.BrokerInfo{{BrokerID: 1}, {BrokerID: 2}},
		Partitions: []api.PartitionAssignment{
			{Topic: "alpha", Partition: 0, Replicas: []int{1, 2}, ISR: []int{1}, Leader: 1},
			{Topic: "alpha", Partition: 1, Replicas: []int{1, 2}, ISR: []int{2}, Leader: 2},
		},
	}

	buildBroker := func(t *testing.T, brokerID int) *Broker {
		t.Helper()
		dir := t.TempDir()

		store, err := storage.NewManager(storage.Config{DataDir: dir})
		if err != nil {
			t.Fatalf("storage: %v", err)
		}

		metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
		if err != nil {
			t.Fatalf("meta store: %v", err)
		}

		if err := metaStore.AppendCreateTopic(ctx, ev); err != nil {
			t.Fatalf("append: %v", err)
		}

		offsetStore, err := NewOffsetStore(dir)
		if err != nil {
			t.Fatalf("offset store: %v", err)
		}

		cluster := &partitionAwareCluster{meta: meta}

		b, err := NewBroker(api.BrokerConfig{
			BrokerID:          brokerID,
			ReplicationFactor: 1,
			DataDir:           dir,
		}, store, offsetStore, metaStore, cluster, nil)
		if err != nil {
			t.Fatalf("broker: %v", err)
		}

		t.Cleanup(func() {
			_ = b.Close()
			_ = store.Close()
			_ = offsetStore.Close()
			_ = metaStore.Close()
		})

		return b
	}

	b1 := buildBroker(t, 1)
	b2 := buildBroker(t, 2)

	if len(b1.topics["alpha"].Partitions) != 2 {
		t.Fatalf("broker1 should load two replicas, got %d", len(b1.topics["alpha"].Partitions))
	}

	if b1.topics["alpha"].Partitions[0].Metadata.Replica.Role != api.RoleLeader {
		t.Fatalf("broker1 partition 0 should be leader")
	}

	if b1.topics["alpha"].Partitions[1].Metadata.Replica.Role != api.RoleFollower {
		t.Fatalf("broker1 partition 1 should be follower")
	}

	if len(b2.topics["alpha"].Partitions) != 2 {
		t.Fatalf("broker2 should load two replicas, got %d", len(b2.topics["alpha"].Partitions))
	}

	if b2.topics["alpha"].Partitions[1].Metadata.Replica.Role != api.RoleLeader {
		t.Fatalf("broker2 partition 1 should be leader")
	}

	if b2.topics["alpha"].Partitions[0].Metadata.Replica.Role != api.RoleFollower {
		t.Fatalf("broker2 partition 0 should be follower")
	}

	assign1, _ := b1.LocalPartitionsSnapshot(ctx)
	if len(assign1) != 1 || assign1[0].Partition != 0 {
		t.Fatalf("broker1 snapshot should include only leader partition 0, got %+v", assign1)
	}

	assign2, _ := b2.LocalPartitionsSnapshot(ctx)
	if len(assign2) != 1 || assign2[0].Partition != 1 {
		t.Fatalf("broker2 snapshot should include only leader partition 1, got %+v", assign2)
	}
}
