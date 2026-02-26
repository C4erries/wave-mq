package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeMetadataStore struct {
	meta api.ClusterMetadata
}

func (f *fakeMetadataStore) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	return f.meta, nil
}

func (f *fakeMetadataStore) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata, 1)

	go func() {
		ch <- f.meta

		<-ctx.Done()
		close(ch)
	}()

	return ch, nil
}

func (f *fakeMetadataStore) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return f.meta, nil
}

func (f *fakeMetadataStore) ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return f.meta, nil
}

func (f *fakeMetadataStore) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

type fakeReplicator struct {
	mu    sync.Mutex
	fetch []FetchRequest
	resp  FetchResponse
	err   error
}

func (f *fakeReplicator) FetchFromLeader(ctx context.Context, leader api.BrokerInfo, req FetchRequest) (FetchResponse, error) {
	_ = ctx
	_ = leader

	f.mu.Lock()
	f.fetch = append(f.fetch, req)
	f.mu.Unlock()

	return f.resp, f.err
}

type trackingReplicator struct {
	mu       sync.Mutex
	leaders  []int
	contexts []context.Context
}

func (t *trackingReplicator) FetchFromLeader(ctx context.Context, leader api.BrokerInfo, req FetchRequest) (FetchResponse, error) {
	_ = req

	t.mu.Lock()
	t.leaders = append(t.leaders, leader.BrokerID)
	t.contexts = append(t.contexts, ctx)
	t.mu.Unlock()

	select {
	case <-ctx.Done():
		return FetchResponse{}, ctx.Err()
	case <-time.After(10 * time.Millisecond):
		return FetchResponse{}, nil
	}
}

func (t *trackingReplicator) hasLeader(id int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, l := range t.leaders {
		if l == id {
			return true
		}
	}

	return false
}

func (t *trackingReplicator) contextsSnapshot() []context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()

	res := make([]context.Context, len(t.contexts))
	copy(res, t.contexts)

	return res
}

type streamMetadataStore struct {
	mu   sync.Mutex
	last api.ClusterMetadata
	feed chan api.ClusterMetadata
}

func newStreamMetadataStore() *streamMetadataStore {
	return &streamMetadataStore{feed: make(chan api.ClusterMetadata, 8)}
}

func (s *streamMetadataStore) push(meta api.ClusterMetadata) {
	s.mu.Lock()
	s.last = meta
	s.mu.Unlock()

	s.feed <- meta
}

func (s *streamMetadataStore) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.last, nil
}

func (s *streamMetadataStore) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	out := make(chan api.ClusterMetadata, 1)

	go func() {
		defer close(out)

		current := sinceVersion

		for {
			select {
			case <-ctx.Done():
				return
			case meta, ok := <-s.feed:
				if !ok {
					return
				}

				if meta.Version <= current {
					continue
				}

				current = meta.Version
				select {
				case <-ctx.Done():
					return
				case out <- meta:
				}
			}
		}
	}()

	return out, nil
}

func (s *streamMetadataStore) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = name
	_ = cfg

	return s.GetClusterMetadata(ctx)
}

func (s *streamMetadataStore) ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error) {
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return s.GetClusterMetadata(ctx)
}

func (s *streamMetadataStore) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func TestManagerStartsReplicatorsForFollowers(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	meta := api.ClusterMetadata{
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
		Partitions: []api.PartitionAssignment{
			{Topic: "a", Partition: 0, Leader: 1, Replicas: []int{1, 2}, ISR: []int{1}},
			{Topic: "b", Partition: 0, Leader: 2, Replicas: []int{2}, ISR: []int{2}},
		},
	}
	ctrl := &fakeMetadataStore{meta: meta}
	repl := &fakeReplicator{}

	mgr := NewManager(api.BrokerConfig{BrokerID: 2}, store, ctrl, repl)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	time.Sleep(300 * time.Millisecond)
	repl.mu.Lock()
	defer repl.mu.Unlock()

	if len(repl.fetch) == 0 {
		t.Fatalf("expected at least one fetch from replicator for follower partition")
	}
	// Ensure we didn't start replication for leader-owned partition.
	for _, req := range repl.fetch {
		if req.Topic == "b" {
			t.Fatalf("should not replicate leader-owned partition b")
		}
	}
}

func TestManagerStartsReplicatorsForMultipleFollowers(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	meta := api.ClusterMetadata{
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
		Partitions: []api.PartitionAssignment{
			{Topic: "a", Partition: 0, Leader: 1, Replicas: []int{1, 2}, ISR: []int{1}},
			{Topic: "a", Partition: 1, Leader: 1, Replicas: []int{1, 2}, ISR: []int{1}},
			{Topic: "a", Partition: 2, Leader: 2, Replicas: []int{1, 2}, ISR: []int{2}}, // local leader, should be skipped
		},
	}
	ctrl := &fakeMetadataStore{meta: meta}
	repl := &fakeReplicator{}

	mgr := NewManager(api.BrokerConfig{BrokerID: 2}, store, ctrl, repl)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		_ = mgr.Run(ctx)
	}()

	time.Sleep(300 * time.Millisecond)
	repl.mu.Lock()
	defer repl.mu.Unlock()

	seen := make(map[int]bool)
	for _, req := range repl.fetch {
		seen[req.Partition] = true
	}

	if !seen[0] || !seen[1] {
		t.Fatalf("expected replication for follower partitions 0 and 1, got %+v", seen)
	}

	if seen[2] {
		t.Fatalf("should not replicate locally-led partition 2")
	}
}

func TestManagerRestartsOnLeaderChange(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	metaFeed := newStreamMetadataStore()
	repl := &trackingReplicator{}

	mgr := NewManager(api.BrokerConfig{BrokerID: 2}, store, metaFeed, repl)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		_ = mgr.Run(ctx)
	}()

	metaFeed.push(api.ClusterMetadata{
		Version:    1,
		Brokers:    []api.BrokerInfo{{BrokerID: 1, Host: "b1"}, {BrokerID: 2, Host: "b2"}, {BrokerID: 3, Host: "b3"}},
		Partitions: []api.PartitionAssignment{{Topic: "a", Partition: 0, Leader: 1, Replicas: []int{1, 2, 3}, ISR: []int{1}}},
	})

	firstCtx := waitForLeaderAndContext(t, repl, 1)

	metaFeed.push(api.ClusterMetadata{
		Version:    2,
		Brokers:    []api.BrokerInfo{{BrokerID: 1, Host: "b1"}, {BrokerID: 2, Host: "b2"}, {BrokerID: 3, Host: "b3"}},
		Partitions: []api.PartitionAssignment{{Topic: "a", Partition: 0, Leader: 3, Replicas: []int{1, 2, 3}, ISR: []int{3}}},
	})

	waitForContextCanceled(firstCtx, t)
	waitForLeader(t, repl, 3)
	waitForNewContext(firstCtx, t, repl)
}

func TestManagerStopsReplicationWhenReplicaRemoved(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })

	metaFeed := newStreamMetadataStore()
	repl := &trackingReplicator{}
	mgr := NewManager(api.BrokerConfig{BrokerID: 2}, store, metaFeed, repl)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		_ = mgr.Run(ctx)
	}()

	metaFeed.push(api.ClusterMetadata{
		Version:    1,
		Brokers:    []api.BrokerInfo{{BrokerID: 1, Host: "b1"}, {BrokerID: 2, Host: "b2"}, {BrokerID: 3, Host: "b3"}},
		Partitions: []api.PartitionAssignment{{Topic: "a", Partition: 0, Leader: 1, Replicas: []int{1, 2, 3}, ISR: []int{1}}},
	})

	firstCtx := waitForLeaderAndContext(t, repl, 1)

	metaFeed.push(api.ClusterMetadata{
		Version:    2,
		Brokers:    []api.BrokerInfo{{BrokerID: 1, Host: "b1"}, {BrokerID: 2, Host: "b2"}, {BrokerID: 3, Host: "b3"}},
		Partitions: []api.PartitionAssignment{{Topic: "a", Partition: 0, Leader: 1, Replicas: []int{1, 3}, ISR: []int{1}}},
	})

	waitForContextCanceled(firstCtx, t)
	ensureNoNewContexts(t, repl, len(repl.contextsSnapshot()))
}

func waitForLeaderAndContext(t *testing.T, repl *trackingReplicator, leader int) context.Context {
	t.Helper()

	deadline := time.After(time.Second)

	for {
		ctxs := repl.contextsSnapshot()
		if repl.hasLeader(leader) && len(ctxs) > 0 {
			return ctxs[0]
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for leader %d fetch", leader)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitForLeader(t *testing.T, repl *trackingReplicator, leader int) {
	t.Helper()

	deadline := time.After(time.Second)

	for {
		if repl.hasLeader(leader) {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for leader %d", leader)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitForContextCanceled(ctx context.Context, t *testing.T) {
	t.Helper()

	deadline := time.After(time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			t.Fatalf("context was not cancelled in time")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitForNewContext(old context.Context, t *testing.T, repl *trackingReplicator) {
	t.Helper()

	deadline := time.After(time.Second)

	for {
		ctxs := repl.contextsSnapshot()
		for _, ctx := range ctxs {
			if ctx != old {
				return
			}
		}

		select {
		case <-deadline:
			t.Fatalf("did not observe new replication context")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func ensureNoNewContexts(t *testing.T, repl *trackingReplicator, expected int) {
	t.Helper()
	time.Sleep(200 * time.Millisecond)

	if got := len(repl.contextsSnapshot()); got != expected {
		t.Fatalf("expected %d contexts after removal, got %d", expected, got)
	}
}
