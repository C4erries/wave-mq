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
	}, store, offsetStore, metaStore, cluster, nil)
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
	}, store, offsetStore, metaStore, cluster, nil)
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

type trackingController struct {
	meta           api.ClusterMetadata
	assignRequests int
}

func (t *trackingController) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx
	return t.meta, nil
}

func (t *trackingController) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata, 1)
	ch <- t.meta

	close(ch)

	return ch, nil
}

func (t *trackingController) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func (t *trackingController) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	t.assignRequests++

	parts := make([]api.PartitionAssignment, 0, cfg.Partitions)
	for pid := 0; pid < cfg.Partitions; pid++ {
		leader := 1
		if pid%2 == 1 {
			leader = 2
		}

		parts = append(parts, api.PartitionAssignment{
			Topic:       name,
			Partition:   pid,
			Replicas:    []int{1, 2},
			ISR:         []int{1, 2},
			Leader:      leader,
			LeaderEpoch: 0,
		})
	}

	t.meta.Partitions = append(t.meta.Partitions, parts...)
	t.meta.Version++

	return t.meta, nil
}

func (t *trackingController) ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return t.meta, nil
}

func TestCreateTopicUsesControllerAssignments(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	if err := store.Recover(context.Background()); err != nil {
		t.Fatalf("storage recover: %v", err)
	}

	offsetStore, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	defer func() {
		_ = store.Close()
		_ = offsetStore.Close()
		_ = metaStore.Close()
	}()

	ctrl := &trackingController{
		meta: api.ClusterMetadata{
			ClusterID: "cluster-track",
			Version:   1,
			Brokers:   []api.BrokerInfo{{BrokerID: 1}, {BrokerID: 2}},
		},
	}
	cfg := api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 2,
		DataDir:           dir,
		ControllerMode:    "raft",
	}

	b, err := NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "alpha", api.TopicConfig{Partitions: 2, ReplicationFactor: 2}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	if ctrl.assignRequests != 1 {
		t.Fatalf("expected one assign request, got %d", ctrl.assignRequests)
	}

	recovered, err := metaStore.RecoverTopics(ctx)
	if err != nil {
		t.Fatalf("recover topics: %v", err)
	}

	topic, ok := recovered.Topics["alpha"]
	if !ok {
		t.Fatalf("expected alpha metadata")
	}

	if len(topic.Partitions) != 2 {
		t.Fatalf("expected broker to log two partitions, got %d", len(topic.Partitions))
	}

	for _, part := range topic.Partitions {
		if len(part.Replicas) != 2 {
			t.Fatalf("expected two replicas in metadata, got %d", len(part.Replicas))
		}

		found1, found2 := false, false

		for _, r := range part.Replicas {
			if int(r.BrokerID) == 1 {
				found1 = true
			}

			if int(r.BrokerID) == 2 {
				found2 = true
			}
		}

		if !found1 || !found2 {
			t.Fatalf("unexpected replicas %v", part.Replicas)
		}
	}

	if detail, ok := b.TopicDetail("alpha"); !ok || detail.PartitionCount != 2 {
		t.Fatalf("expected topic detail with two partitions, got %+v", detail)
	}
}

func TestBrokerUpdatesPartitionsOnClusterMetadataChange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	if err := store.Recover(ctx); err != nil {
		t.Fatalf("storage recover: %v", err)
	}

	offsetStore, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	defer func() {
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()

	cfg := api.BrokerConfig{BrokerID: 1, ReplicationFactor: 1, DataDir: dir}
	ctrl := &trackingController{
		meta: api.ClusterMetadata{Brokers: []api.BrokerInfo{{BrokerID: 1}, {BrokerID: 2}}},
	}

	b, err := NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	meta1 := api.ClusterMetadata{
		Version: 1,
		Brokers: []api.BrokerInfo{{BrokerID: 1}, {BrokerID: 2}},
		Partitions: []api.PartitionAssignment{{
			Topic:       "alpha",
			Partition:   0,
			Replicas:    []int{1, 2},
			ISR:         []int{1},
			Leader:      1,
			LeaderEpoch: 1,
		}},
	}
	if err := b.handleClusterMetadataUpdate(ctx, meta1); err != nil {
		t.Fatalf("apply meta1: %v", err)
	}

	topic, ok := b.topics["alpha"]
	if !ok {
		t.Fatalf("expected topic alpha after meta1")
	}

	part0, ok := topic.Partitions[0]
	if !ok {
		t.Fatalf("expected partition 0 for alpha")
	}

	if part0.Metadata.Leader != 1 || part0.Metadata.Replica.Role != api.RoleLeader {
		t.Fatalf("unexpected initial metadata %+v", part0.Metadata)
	}

	meta2 := api.ClusterMetadata{
		Version: 2,
		Brokers: []api.BrokerInfo{{BrokerID: 1}, {BrokerID: 2}},
		Partitions: []api.PartitionAssignment{
			{
				Topic:       "alpha",
				Partition:   0,
				Replicas:    []int{1, 2},
				ISR:         []int{1, 2},
				Leader:      2,
				LeaderEpoch: 2,
			},
			{
				Topic:       "alpha",
				Partition:   1,
				Replicas:    []int{1, 2},
				ISR:         []int{2},
				Leader:      2,
				LeaderEpoch: 1,
			},
		},
	}
	if err := b.handleClusterMetadataUpdate(ctx, meta2); err != nil {
		t.Fatalf("apply meta2: %v", err)
	}

	topic = b.topics["alpha"]

	part0 = topic.Partitions[0]
	if part0.Metadata.Leader != 2 || part0.Metadata.Replica.Role != api.RoleFollower {
		t.Fatalf("expected follower metadata after meta2, got %+v", part0.Metadata)
	}

	part1, ok := topic.Partitions[1]
	if !ok {
		t.Fatalf("expected new partition1 for alpha")
	}

	if part1.Metadata.Leader != 2 || part1.Metadata.Replica.Role != api.RoleFollower {
		t.Fatalf("unexpected metadata for partition1: %+v", part1.Metadata)
	}
}
