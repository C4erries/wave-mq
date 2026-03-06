package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func (f *fakeMetadataStore) ReportReplicaProgress(
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

type fakeWorker struct {
	runCh chan struct{}
}

func (w *fakeWorker) Run(ctx context.Context) error {
	close(w.runCh)
	<-ctx.Done()

	return ctx.Err()
}

type workerFunc func(context.Context) error

func (fn workerFunc) Run(ctx context.Context) error {
	return fn(ctx)
}

type noopSink struct{}

func (s *noopSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = ctx
	_ = records
	_ = highWatermark

	return -1, nil
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

func (s *streamMetadataStore) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
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

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})

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

	errCh := runManagerAsync(mgr, ctx)

	waitForCondition(t, time.Second, func() bool {
		repl.mu.Lock()
		defer repl.mu.Unlock()

		return len(repl.fetch) > 0
	}, "expected at least one fetch from replicator for follower partition")

	repl.mu.Lock()
	defer repl.mu.Unlock()

	// Ensure we didn't start replication for leader-owned partition.
	for _, req := range repl.fetch {
		if req.Topic == "b" {
			t.Fatalf("should not replicate leader-owned partition b")
		}
	}

	cancel()
	assertManagerRunEnded(t, errCh)
}

func TestManagerStartsReplicatorsForMultipleFollowers(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})

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

	errCh := runManagerAsync(mgr, ctx)

	waitForCondition(t, time.Second, func() bool {
		repl.mu.Lock()
		defer repl.mu.Unlock()

		seen := make(map[int]bool)
		for _, req := range repl.fetch {
			seen[req.Partition] = true
		}

		return seen[0] && seen[1]
	}, "expected replication for follower partitions 0 and 1")

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

	cancel()
	assertManagerRunEnded(t, errCh)
}

func TestManagerRestartsOnLeaderChange(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})

	metaFeed := newStreamMetadataStore()
	repl := &trackingReplicator{}

	mgr := NewManager(api.BrokerConfig{BrokerID: 2}, store, metaFeed, repl)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := runManagerAsync(mgr, ctx)

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

	cancel()
	assertManagerRunEnded(t, errCh)
}

func TestManagerStopsReplicationWhenReplicaRemoved(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})

	metaFeed := newStreamMetadataStore()
	repl := &trackingReplicator{}
	mgr := NewManager(api.BrokerConfig{BrokerID: 2}, store, metaFeed, repl)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := runManagerAsync(mgr, ctx)

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

	cancel()
	assertManagerRunEnded(t, errCh)
}

func TestManagerUsesInjectedFactories(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})

	metaFeed := newStreamMetadataStore()
	repl := &trackingReplicator{}
	created := make(chan struct{}, 1)

	mgr := NewManagerWithFactories(
		api.BrokerConfig{BrokerID: 2},
		store,
		metaFeed,
		repl,
		func(topic string, partition int) Sink {
			_ = topic
			_ = partition

			return &noopSink{}
		},
		func(repl Replicator, leader api.BrokerInfo, topic string, partition int, sink Sink) ReplicationWorker {
			_ = repl
			_ = leader
			_ = topic
			_ = partition
			_ = sink

			created <- struct{}{}

			return &fakeWorker{runCh: make(chan struct{})}
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := runManagerAsync(mgr, ctx)

	metaFeed.push(api.ClusterMetadata{
		Version:    1,
		Brokers:    []api.BrokerInfo{{BrokerID: 1, Host: "b1"}, {BrokerID: 2, Host: "b2"}},
		Partitions: []api.PartitionAssignment{{Topic: "x", Partition: 0, Leader: 1, Replicas: []int{1, 2}, ISR: []int{1}}},
	})

	select {
	case <-created:
	case <-time.After(time.Second):
		t.Fatalf("expected worker factory to be invoked")
	}

	cancel()
	assertManagerRunEnded(t, errCh)
}

func TestManagerRestartsWorkerAfterFailure(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	}()

	metaFeed := newStreamMetadataStore()

	var attempts atomic.Int32

	workerStarted := make(chan struct{}, 1)

	mgr := NewManagerWithFactories(
		api.BrokerConfig{BrokerID: 2},
		store,
		metaFeed,
		&trackingReplicator{},
		func(topic string, partition int) Sink {
			_ = topic
			_ = partition

			return &noopSink{}
		},
		func(repl Replicator, leader api.BrokerInfo, topic string, partition int, sink Sink) ReplicationWorker {
			_ = repl
			_ = leader
			_ = topic
			_ = partition
			_ = sink

			attempt := attempts.Add(1)
			if attempt == 1 {
				return workerFunc(func(ctx context.Context) error {
					_ = ctx

					return errors.New("transient failure")
				})
			}

			return workerFunc(func(ctx context.Context) error {
				select {
				case workerStarted <- struct{}{}:
				default:
				}

				<-ctx.Done()

				return ctx.Err()
			})
		},
	)
	mgr.restartBackoff = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runManagerAsync(mgr, ctx)

	metaFeed.push(api.ClusterMetadata{
		Version: 1,
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
		Partitions: []api.PartitionAssignment{
			{Topic: "r", Partition: 0, Leader: 1, Replicas: []int{1, 2}, ISR: []int{1}},
		},
	})

	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatalf("expected restarted worker to start")
	}

	if attempts.Load() < 2 {
		t.Fatalf("expected at least two worker attempts, got %d", attempts.Load())
	}

	cancel()
	assertManagerRunEnded(t, errCh)
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

	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if got := len(repl.contextsSnapshot()); got != expected {
			t.Fatalf("expected %d contexts after removal, got %d", expected, got)
		}

		select {
		case <-timer.C:
			return
		case <-ticker.C:
		}
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, pred func() bool, failMsg string) {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if pred() {
			return
		}

		select {
		case <-timer.C:
			t.Fatalf("%s", failMsg)
		case <-ticker.C:
		}
	}
}

func runManagerAsync(mgr *Manager, ctx context.Context) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- mgr.Run(ctx)
	}()

	return errCh
}

func assertManagerRunEnded(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("manager run failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("manager did not stop in time")
	}
}
