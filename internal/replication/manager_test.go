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
	return f.meta, nil
}

func (f *fakeMetadataStore) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	ch := make(chan api.ClusterMetadata, 1)
	go func() {
		ch <- f.meta
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeMetadataStore) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	return f.meta, nil
}

func (f *fakeMetadataStore) ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error) {
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
	f.mu.Lock()
	f.fetch = append(f.fetch, req)
	f.mu.Unlock()
	return f.resp, f.err
}

func TestManagerStartsReplicatorsForFollowers(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
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

	go mgr.Run(ctx)
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
