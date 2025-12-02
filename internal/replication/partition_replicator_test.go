package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeSink struct {
	records       []api.Record
	highWatermark api.Offset
}

type testReplicator struct {
	mu    sync.Mutex
	fetch []FetchRequest
	resp  FetchResponse
	err   error
}

func (f *testReplicator) FetchFromLeader(ctx context.Context, leader api.BrokerInfo, req FetchRequest) (FetchResponse, error) {
	f.mu.Lock()
	f.fetch = append(f.fetch, req)
	f.mu.Unlock()
	return f.resp, f.err
}

func (f *fakeSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = ctx
	f.records = append(f.records, records...)
	f.highWatermark = highWatermark
	if len(records) == 0 {
		return -1, nil
	}
	return records[len(records)-1].Offset, nil
}

func TestPartitionReplicatorFetchesBatches(t *testing.T) {
	b := newFakeBroker()
	if err := b.CreateTopic(context.Background(), "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	_, _ = b.Produce(context.Background(), "alpha", 0, []api.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
	})

	server, err := netproto.NewServer("127.0.0.1:0", b)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = server.ListenAndServe(ctx)
	}()
	addr := waitForAddr(t, server)

	sink := &fakeSink{}
	rep := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: addr.String()}, "alpha", 0, sink)
	rep.Interval = 10 * time.Millisecond
	done := make(chan struct{})
	go func() {
		_ = rep.Run(ctx)
		close(done)
	}()

	waitUntil(t, func() bool { return len(sink.records) == 2 }, 2*time.Second)
	cancel()
	<-done
	if sink.highWatermark != 1 {
		t.Fatalf("expected hwm 1, got %d", sink.highWatermark)
	}
	if string(sink.records[0].Value) != "one" || string(sink.records[1].Value) != "two" {
		t.Fatalf("unexpected records: %+v", sink.records)
	}
}

func TestPartitionReplicatorStopsOnError(t *testing.T) {
	sink := &fakeSink{}
	rep := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: "127.0.0.1:1"}, "missing", 0, sink)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := rep.Run(ctx)
	if err == nil {
		t.Fatalf("expected error from replicator")
	}
}

func TestPartitionReplicatorWritesToWAL(t *testing.T) {
	ctx := context.Background()
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leaderStore, err := storage.NewManager(storage.Config{DataDir: leaderDir})
	if err != nil {
		t.Fatalf("leader storage: %v", err)
	}
	defer leaderStore.Close()
	if err := leaderStore.Recover(ctx); err != nil {
		t.Fatalf("leader recover: %v", err)
	}
	leaderOffset, _ := broker.NewOffsetStore(leaderDir)
	defer leaderOffset.Close()
	leaderMeta, _ := metadata.NewStore(api.BrokerConfig{DataDir: leaderDir})
	defer leaderMeta.Close()
	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           leaderDir,
		ReplicationFactor: 1,
	}, leaderStore, leaderOffset, leaderMeta, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()
	if err := b.CreateTopic(ctx, "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := b.Produce(ctx, "alpha", 0, []api.Record{{Value: []byte("one")}, {Value: []byte("two")}}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	server, err := netproto.NewServer("127.0.0.1:0", b)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ctxSrv, cancelSrv := context.WithCancel(ctx)
	defer cancelSrv()
	go func() {
		_ = server.ListenAndServe(ctxSrv)
	}()
	addr := waitForAddr(t, server)

	followerStore, err := storage.NewManager(storage.Config{DataDir: followerDir})
	if err != nil {
		t.Fatalf("follower storage: %v", err)
	}
	defer followerStore.Close()
	if err := followerStore.Recover(ctx); err != nil {
		t.Fatalf("follower recover: %v", err)
	}
	sink := NewWALSink(followerStore, "alpha", 0)
	ctrl := &fakeController{}
	sink = NewReportingSink(sink, ctrl, "alpha", 0, 2)

	rep := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: addr.String()}, "alpha", 0, sink)
	rep.Interval = 10 * time.Millisecond
	ctxRep, cancelRep := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = rep.Run(ctxRep)
		close(done)
	}()

	followerLog, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	waitUntil(t, func() bool { return followerLog.HighWatermark() == 1 }, 3*time.Second)
	cancelRep()
	<-done
	cancelSrv()

	leaderLog, _ := leaderStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	defer leaderLog.Close()
	defer followerLog.Close()
	if followerLog.HighWatermark() != leaderLog.HighWatermark() {
		t.Fatalf("expected follower hwm %d to match leader %d", followerLog.HighWatermark(), leaderLog.HighWatermark())
	}
	records, _ := followerLog.Read(ctx, 0, 1<<20)
	if len(records) != 2 || string(records[0].Value) != "one" || string(records[1].Value) != "two" {
		t.Fatalf("unexpected follower records: %+v", records)
	}
	if len(ctrl.calls) == 0 || ctrl.calls[len(ctrl.calls)-1].last != followerLog.HighWatermark() {
		t.Fatalf("expected reporting sink to report last offset %d, got %+v", followerLog.HighWatermark(), ctrl.calls)
	}
}

type offsetSink struct {
	next    api.Offset
	applied int
}

func (s *offsetSink) NextOffset() (api.Offset, error) { return s.next, nil }

func (s *offsetSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = ctx
	_ = highWatermark
	base := s.next
	last := base + api.Offset(len(records)) - 1
	s.next = last + 1
	s.applied += len(records)
	return last, nil
}

func TestPartitionReplicatorUsesNextOffsetHint(t *testing.T) {
	fr := &testReplicator{resp: FetchResponse{
		Records: []api.Record{{Value: []byte("x")}},
	}}
	sink := &offsetSink{next: 5}
	rep := NewPartitionReplicator(fr, api.BrokerInfo{Host: "ignored"}, "t", 0, sink)
	rep.Interval = 0
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = rep.Run(ctx)
		close(done)
	}()
	waitUntil(t, func() bool { return sink.applied > 0 }, time.Second)
	cancel()
	<-done
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if len(fr.fetch) == 0 || fr.fetch[0].Offset != 5 {
		t.Fatalf("expected first fetch from offset 5, got %+v", fr.fetch)
	}
}

func waitUntil(t *testing.T, pred func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
