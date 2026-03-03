package broker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func assertClose(t *testing.T, target string, err error) {
	t.Helper()

	if err != nil && !errors.Is(err, os.ErrClosed) {
		t.Errorf("close %s: %v", target, err)
	}
}

func requireClose(t *testing.T, target string, err error) {
	t.Helper()

	if err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close %s: %v", target, err)
	}
}

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

	offsetStore, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	b, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore, nil, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	cleanup := func() {
		assertClose(t, "broker", b.Close())
		assertClose(t, "storage", store.Close())
		assertClose(t, "offset store", offsetStore.Close())
		assertClose(t, "metadata store", metaStore.Close())
	}

	return b, cleanup
}

type fakeClusterStore struct {
	mu     sync.RWMutex
	meta   api.ClusterMetadata
	getErr error
}

type blockingClusterStore struct {
	wait <-chan struct{}
}

func (bcs *blockingClusterStore) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	select {
	case <-ctx.Done():
		return api.ClusterMetadata{}, ctx.Err()
	case <-bcs.wait:
		return api.ClusterMetadata{}, nil
	}
}

func (bcs *blockingClusterStore) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata)
	close(ch)

	return ch, nil
}

func (bcs *blockingClusterStore) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func (bcs *blockingClusterStore) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return api.ClusterMetadata{}, nil
}

func (bcs *blockingClusterStore) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition int,
	brokerID int,
	lastOffset api.Offset,
	leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return api.ClusterMetadata{}, nil
}

func (f *fakeClusterStore) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	if f.getErr != nil {
		return api.ClusterMetadata{}, f.getErr
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.meta, nil
}

func (f *fakeClusterStore) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata)
	close(ch)

	return ch, nil
}

func (f *fakeClusterStore) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func (f *fakeClusterStore) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return api.ClusterMetadata{}, nil
}

func (f *fakeClusterStore) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition int,
	brokerID int,
	lastOffset api.Offset,
	leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return api.ClusterMetadata{}, nil
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

		if !bytes.Equal(r.Value, records[i].Value) {
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

	off, err = b.FetchCommitted(ctx, "g1", "t", 0)
	if err != nil {
		t.Fatalf("fetch committed after regression: %v", err)
	}

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

func TestOffsetPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
		SyncOnAppend:    true,
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

	b, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore, nil, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	ctx := context.Background()
	if err := b.CommitOffset(ctx, "g", "t", 0, 5); err != nil {
		t.Fatalf("commit: %v", err)
	}

	requireClose(t, "broker", b.Close())
	requireClose(t, "storage", store.Close())
	requireClose(t, "offset store", offsetStore.Close())
	requireClose(t, "metadata store", metaStore.Close())

	// Reopen and ensure offsets persist.
	store, err = storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("storage reopen: %v", err)
	}

	offsetStore, err = NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store reopen: %v", err)
	}

	metaStore, err = metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store reopen: %v", err)
	}

	b, err = NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore, nil, nil)
	if err != nil {
		t.Fatalf("broker reopen: %v", err)
	}

	defer func() {
		assertClose(t, "broker", b.Close())
		assertClose(t, "storage", store.Close())
		assertClose(t, "offset store", offsetStore.Close())
		assertClose(t, "metadata store", metaStore.Close())
	}()

	off, err := b.FetchCommitted(ctx, "g", "t", 0)
	if err != nil {
		t.Fatalf("fetch committed: %v", err)
	}

	if off != 5 {
		t.Fatalf("expected offset 5 after restart, got %d", off)
	}
}

func TestProduceByKeyRoutesSameKeyToSamePartition(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "keyed", api.TopicConfig{Partitions: 3}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	partition1, _, err := b.ProduceByKey(ctx, "keyed", []byte("sensor-42"), []api.Record{{Value: []byte("one")}})
	if err != nil {
		t.Fatalf("produce by key #1: %v", err)
	}

	partition2, _, err := b.ProduceByKey(ctx, "keyed", []byte("sensor-42"), []api.Record{{Value: []byte("two")}})
	if err != nil {
		t.Fatalf("produce by key #2: %v", err)
	}

	if partition1 != partition2 {
		t.Fatalf("expected same key to map to same partition, got %d and %d", partition1, partition2)
	}

	recs, err := b.Fetch(ctx, "keyed", partition1, 0, 0)
	if err != nil {
		t.Fatalf("fetch keyed partition: %v", err)
	}

	if len(recs) != 2 {
		t.Fatalf("expected 2 records in routed partition, got %d", len(recs))
	}
}

func TestCreateTopicAppliesRetentionOverrides(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 128,
		SyncOnAppend:    true,
		MaxLogBytes:     -1,
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

	b, err := NewBroker(api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}, store, offsetStore, metaStore, nil, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	defer func() {
		assertClose(t, "broker", b.Close())
		assertClose(t, "storage", store.Close())
		assertClose(t, "offset store", offsetStore.Close())
		assertClose(t, "metadata store", metaStore.Close())
	}()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "retained", api.TopicConfig{
		Partitions:     1,
		RetentionBytes: 256,
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	payload := []byte(strings.Repeat("x", 64))
	for i := 0; i < 60; i++ {
		if _, err := b.Produce(ctx, "retained", 0, []api.Record{{Value: payload}}); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	detail, ok := b.TopicDetail("retained")
	if !ok {
		t.Fatalf("topic detail not found")
	}

	if len(detail.Partitions) != 1 {
		t.Fatalf("expected one partition, got %d", len(detail.Partitions))
	}

	if detail.Partitions[0].StartOffset <= 0 {
		t.Fatalf("expected retention to advance start offset, got %d", detail.Partitions[0].StartOffset)
	}
}

func TestTopicPartitionIDsFallbackOnClusterMetadataError(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "fallback", api.TopicConfig{Partitions: 2}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	b.cluster = &fakeClusterStore{getErr: context.DeadlineExceeded}

	parts := b.topicPartitionIDs("fallback")
	if len(parts) != 2 || parts[0] != 0 || parts[1] != 1 {
		t.Fatalf("unexpected fallback partition ids: %#v", parts)
	}
}

func TestTopicPartitionIDsUseClusterAssignmentsWhenAvailable(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "clustered", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	b.cluster = &fakeClusterStore{
		meta: api.ClusterMetadata{
			Partitions: []api.PartitionAssignment{
				{Topic: "clustered", Partition: 2, Leader: 1, Replicas: []int{1}},
				{Topic: "clustered", Partition: 0, Leader: 1, Replicas: []int{1}},
			},
		},
	}

	parts := b.topicPartitionIDs("clustered")
	if len(parts) != 2 || parts[0] != 0 || parts[1] != 2 {
		t.Fatalf("unexpected cluster partition ids: %#v", parts)
	}
}

func TestProduceFetchConcurrentWithMetadataUpdates(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "race", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	b.cluster = &fakeClusterStore{
		meta: api.ClusterMetadata{
			Partitions: []api.PartitionAssignment{
				{
					Topic:     "race",
					Partition: 0,
					Leader:    1,
					Replicas:  []int{1, 2},
					ISR:       []int{1, 2},
				},
			},
		},
	}

	assignLeader := map[string]map[int]api.PartitionAssignment{
		"race": {
			0: {
				Topic:     "race",
				Partition: 0,
				Leader:    1,
				Replicas:  []int{1, 2},
				ISR:       []int{1, 2},
			},
		},
	}
	assignFollower := map[string]map[int]api.PartitionAssignment{
		"race": {
			0: {
				Topic:     "race",
				Partition: 0,
				Leader:    2,
				Replicas:  []int{1, 2},
				ISR:       []int{1, 2},
			},
		},
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < 2000; i++ {
			if i%2 == 0 {
				b.updatePartitionMetadata(assignLeader)
			} else {
				b.updatePartitionMetadata(assignFollower)
			}
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < 2000; i++ {
			if _, err := b.Produce(ctx, "race", 0, []api.Record{{Value: []byte("v")}}); err != nil && !errors.Is(err, ErrNotLeader) {
				errCh <- err

				return
			}
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < 2000; i++ {
			if _, err := b.Fetch(ctx, "race", 0, 0, 1024); err != nil && !errors.Is(err, ErrNotLeader) {
				errCh <- err

				return
			}
		}
	}()

	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("unexpected produce/fetch error: %v", err)
	default:
	}
}

func TestProduceFetchNotLeaderDoesNotBlockOnClusterMetadata(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "blocked", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	assignFollower := map[string]map[int]api.PartitionAssignment{
		"blocked": {
			0: {
				Topic:     "blocked",
				Partition: 0,
				Leader:    2,
				Replicas:  []int{1, 2},
				ISR:       []int{1, 2},
			},
		},
	}
	b.updatePartitionMetadata(assignFollower)
	b.cluster = &blockingClusterStore{wait: make(chan struct{})}

	produceDone := make(chan error, 1)
	go func() {
		_, err := b.Produce(ctx, "blocked", 0, []api.Record{{Value: []byte("v")}})
		produceDone <- err
	}()

	select {
	case err := <-produceDone:
		var notLeader NotLeaderError
		if err == nil || !errors.As(err, &notLeader) {
			t.Fatalf("expected NotLeaderError from produce, got %v", err)
		}

		if notLeader.Leader != 2 {
			t.Fatalf("expected leader hint 2, got %d", notLeader.Leader)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("produce blocked waiting for cluster metadata")
	}

	fetchDone := make(chan error, 1)
	go func() {
		_, err := b.Fetch(ctx, "blocked", 0, 0, 1024)
		fetchDone <- err
	}()

	select {
	case err := <-fetchDone:
		var notLeader NotLeaderError
		if err == nil || !errors.As(err, &notLeader) {
			t.Fatalf("expected NotLeaderError from fetch, got %v", err)
		}

		if notLeader.Leader != 2 {
			t.Fatalf("expected leader hint 2, got %d", notLeader.Leader)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("fetch blocked waiting for cluster metadata")
	}
}

func TestConsumerGroupsSnapshotFallsBackToLocalHighWatermarks(t *testing.T) {
	b, cleanup := newTestBroker(t)
	defer cleanup()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "snap", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	if _, err := b.Produce(ctx, "snap", 0, []api.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
		{Value: []byte("three")},
	}); err != nil {
		t.Fatalf("produce records: %v", err)
	}

	if err := b.CommitOffset(ctx, "group-1", "snap", 0, 0); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	b.cluster = &fakeClusterStore{getErr: context.DeadlineExceeded}

	snapshot := b.ConsumerGroupsSnapshot(ctx)
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 group in snapshot, got %d", len(snapshot))
	}

	assignments := snapshot[0].Assignments
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}

	if assignments[0].HighWatermark != 2 {
		t.Fatalf("expected high watermark 2 from local fallback, got %d", assignments[0].HighWatermark)
	}

	if assignments[0].Lag != 1 {
		t.Fatalf("expected lag 1, got %d", assignments[0].Lag)
	}
}
