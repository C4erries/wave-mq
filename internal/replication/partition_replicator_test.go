package replication

import (
	"context"
	"errors"
	"os"
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
	mu            sync.Mutex
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
	_ = ctx
	_ = leader

	f.mu.Lock()
	f.fetch = append(f.fetch, req)
	f.mu.Unlock()

	return f.resp, f.err
}

func (f *fakeSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = ctx

	f.mu.Lock()
	defer f.mu.Unlock()

	f.records = append(f.records, records...)
	f.highWatermark = highWatermark

	if len(records) == 0 {
		return -1, nil
	}

	return records[len(records)-1].Offset, nil
}

func (f *fakeSink) snapshot() ([]api.Record, api.Offset) {
	f.mu.Lock()
	defer f.mu.Unlock()

	recs := append([]api.Record(nil), f.records...)

	return recs, f.highWatermark
}

func TestPartitionReplicatorFetchesBatches(t *testing.T) {
	b := newFakeBroker()
	if err := b.CreateTopic(context.Background(), "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	if _, err := b.Produce(context.Background(), "alpha", 0, []api.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	server, err := netproto.NewServer("127.0.0.1:0", b)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe(ctx)
	}()

	defer assertAsyncError(t, serverErr, context.Canceled)

	addr := waitForAddr(t, server)

	sink := &fakeSink{}
	rep := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: addr.String()}, "alpha", 0, sink)
	rep.Interval = 10 * time.Millisecond
	done := make(chan struct{})
	repErr := make(chan error, 1)

	go func() {
		repErr <- rep.Run(ctx)

		close(done)
	}()

	waitUntil(t, func() bool {
		recs, _ := sink.snapshot()
		return len(recs) == 2
	}, 2*time.Second)
	cancel()
	<-done
	assertAsyncError(t, repErr, context.Canceled)

	records, hwm := sink.snapshot()
	if hwm != 1 {
		t.Fatalf("expected hwm 1, got %d", hwm)
	}

	if string(records[0].Value) != "one" || string(records[1].Value) != "two" {
		t.Fatalf("unexpected records: %+v", records)
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

func TestPartitionReplicatorRunRequiresReplicator(t *testing.T) {
	rep := NewPartitionReplicator(nil, api.BrokerInfo{Host: "127.0.0.1:1"}, "alpha", 0, &fakeSink{})

	err := rep.Run(context.Background())
	if err == nil {
		t.Fatalf("expected error for nil replicator")
	}
}

func TestPartitionReplicatorRunRequiresSink(t *testing.T) {
	rep := NewPartitionReplicator(&testReplicator{}, api.BrokerInfo{Host: "127.0.0.1:1"}, "alpha", 0, nil)

	err := rep.Run(context.Background())
	if err == nil {
		t.Fatalf("expected error for nil sink")
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

	defer func() {
		closeExpectNoErr(t, "leader storage", leaderStore.Close())
	}()

	if err := leaderStore.Recover(ctx); err != nil {
		t.Fatalf("leader recover: %v", err)
	}

	leaderOffset, err := broker.NewOffsetStore(leaderDir)
	if err != nil {
		t.Fatalf("leader offset store: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "leader offset store", leaderOffset.Close())
	}()

	leaderMeta, err := metadata.NewStore(api.BrokerConfig{DataDir: leaderDir})
	if err != nil {
		t.Fatalf("leader metadata store: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "leader metadata store", leaderMeta.Close())
	}()

	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           leaderDir,
		ReplicationFactor: 1,
	}, leaderStore, leaderOffset, leaderMeta, nil, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "leader broker", b.Close())
	}()

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

	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe(ctxSrv)
	}()

	addr := waitForAddr(t, server)

	followerStore, err := storage.NewManager(storage.Config{DataDir: followerDir})
	if err != nil {
		t.Fatalf("follower storage: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "follower storage", followerStore.Close())
	}()

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
	repErr := make(chan error, 1)

	go func() {
		repErr <- rep.Run(ctxRep)

		close(done)
	}()

	followerLog, err := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	if err != nil {
		t.Fatalf("open follower log: %v", err)
	}

	waitUntil(t, func() bool { return followerLog.HighWatermark() == 1 }, 3*time.Second)
	cancelRep()
	<-done
	assertAsyncError(t, repErr, context.Canceled)
	cancelSrv()
	assertAsyncError(t, serverErr, context.Canceled)

	leaderLog, err := leaderStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	if err != nil {
		t.Fatalf("open leader log: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "leader log", leaderLog.Close())
	}()
	defer func() {
		closeExpectNoErr(t, "follower log", followerLog.Close())
	}()

	if followerLog.HighWatermark() != leaderLog.HighWatermark() {
		t.Fatalf("expected follower hwm %d to match leader %d", followerLog.HighWatermark(), leaderLog.HighWatermark())
	}

	records, err := followerLog.Read(ctx, 0, 1<<20)
	if err != nil {
		t.Fatalf("read follower log: %v", err)
	}

	if len(records) != 2 || string(records[0].Value) != "one" || string(records[1].Value) != "two" {
		t.Fatalf("unexpected follower records: %+v", records)
	}

	if len(ctrl.calls) == 0 || ctrl.calls[len(ctrl.calls)-1].last != followerLog.HighWatermark() {
		t.Fatalf("expected reporting sink to report last offset %d, got %+v", followerLog.HighWatermark(), ctrl.calls)
	}
}

type offsetSink struct {
	mu      sync.Mutex
	next    api.Offset
	applied int
}

func (s *offsetSink) NextOffset() (api.Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.next, nil
}

func (s *offsetSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = ctx
	_ = highWatermark

	s.mu.Lock()
	defer s.mu.Unlock()

	base := s.next
	last := base + api.Offset(len(records)) - 1
	s.next = last + 1
	s.applied += len(records)

	return last, nil
}

func (s *offsetSink) Applied() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.applied
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
	runErr := make(chan error, 1)

	go func() {
		runErr <- rep.Run(ctx)

		close(done)
	}()

	waitUntil(t, func() bool { return sink.Applied() > 0 }, time.Second)
	cancel()
	<-done
	assertAsyncError(t, runErr, context.Canceled)
	fr.mu.Lock()
	defer fr.mu.Unlock()

	if len(fr.fetch) == 0 || fr.fetch[0].Offset != 5 {
		t.Fatalf("expected first fetch from offset 5, got %+v", fr.fetch)
	}
}

func TestPartitionReplicatorResumesFromNextOffset(t *testing.T) {
	ctx := context.Background()
	initial := []string{"msg-0", "msg-1"}
	newRecords := []string{"msg-2", "msg-3"}

	addr, cleanup, leader := startLeader(ctx, t)
	defer cleanup()

	if err := leader.CreateTopic(ctx, "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	produceValues(ctx, t, leader, "alpha", 0, initial)

	followerStore, err := storage.NewManager(storage.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("follower storage: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "follower storage", followerStore.Close())
	}()

	if err := followerStore.Recover(ctx); err != nil {
		t.Fatalf("follower recover: %v", err)
	}

	ws1 := NewWALSink(followerStore, "alpha", 0).(*walSink)
	ctrl := &fakeController{}
	sink1 := NewReportingSink(ws1, ctrl, "alpha", 0, 2)
	rep1 := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: addr}, "alpha", 0, sink1)
	initialTarget := api.Offset(len(initial) - 1)
	runReplicatorUntil(ctx, t, rep1, ws1, initialTarget)

	callsBefore := len(ctrl.calls)

	produceValues(ctx, t, leader, "alpha", 0, newRecords)
	total := append(append([]string(nil), initial...), newRecords...)
	finalHWM := api.Offset(len(total) - 1)

	ws2 := NewWALSink(followerStore, "alpha", 0).(*walSink)

	offset, err := ws2.NextOffset()
	if err != nil {
		t.Fatalf("next offset: %v", err)
	}

	if offset != api.Offset(len(initial)) {
		t.Fatalf("expected next offset %d after restart, got %d", len(initial), offset)
	}

	sink2 := NewReportingSink(ws2, ctrl, "alpha", 0, 2)
	rep2 := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: addr}, "alpha", 0, sink2)
	runReplicatorUntil(ctx, t, rep2, ws2, finalHWM)

	records := readLogValues(ctx, t, followerStore)
	if len(records) != len(total) {
		t.Fatalf("expected %d records, got %d", len(total), len(records))
	}

	for i, rec := range records {
		if rec != total[i] {
			t.Fatalf("expected record %s at %d, got %s", total[i], i, rec)
		}
	}

	if len(ctrl.calls) <= callsBefore {
		t.Fatalf("expected reporting sink to record progress after restart, got %+v", ctrl.calls)
	}

	lastCall := ctrl.calls[len(ctrl.calls)-1]
	if lastCall.last != finalHWM || lastCall.hwm != finalHWM {
		t.Fatalf("expected last/hwm %d, got %+v", finalHWM, lastCall)
	}
}

func TestPartitionReplicatorDoesNotDuplicateAfterCatchUp(t *testing.T) {
	ctx := context.Background()
	values := []string{"foo", "bar", "baz"}

	addr, cleanup, leader := startLeader(ctx, t)
	defer cleanup()

	if err := leader.CreateTopic(ctx, "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	produceValues(ctx, t, leader, "alpha", 0, values)
	finalHWM := api.Offset(len(values) - 1)

	followerStore, err := storage.NewManager(storage.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("follower storage: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "follower storage", followerStore.Close())
	}()

	if err := followerStore.Recover(ctx); err != nil {
		t.Fatalf("follower recover: %v", err)
	}

	ctrl := &fakeController{}
	ws1 := NewWALSink(followerStore, "alpha", 0).(*walSink)
	sink := NewReportingSink(ws1, ctrl, "alpha", 0, 2)
	rep := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: addr}, "alpha", 0, sink)
	runReplicatorUntil(ctx, t, rep, ws1, finalHWM)

	callsBefore := len(ctrl.calls)

	ws2 := NewWALSink(followerStore, "alpha", 0).(*walSink)

	next, err := ws2.NextOffset()
	if err != nil {
		t.Fatalf("next offset: %v", err)
	}

	if next != finalHWM+1 {
		t.Fatalf("expected next offset %d, got %d", finalHWM+1, next)
	}

	sink2 := NewReportingSink(ws2, ctrl, "alpha", 0, 2)
	rep2 := NewPartitionReplicator(NewBinaryReplicator(), api.BrokerInfo{Host: addr}, "alpha", 0, sink2)

	ctx2, cancel2 := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel2()

	done := make(chan struct{})
	runErr := make(chan error, 1)

	go func() {
		runErr <- rep2.Run(ctx2)

		close(done)
	}()

	<-done
	assertAsyncError(t, runErr, context.DeadlineExceeded)

	records := readLogValues(ctx, t, followerStore)
	if len(records) != len(values) {
		t.Fatalf("expected %d records after restart, got %d", len(values), len(records))
	}

	if len(ctrl.calls) != callsBefore {
		t.Fatalf("expected no extra controller reports, got %d", len(ctrl.calls))
	}
}

func startLeader(ctx context.Context, t *testing.T) (string, func(), *broker.Broker) {
	t.Helper()
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("leader storage: %v", err)
	}

	if err := store.Recover(ctx); err != nil {
		t.Fatalf("leader recover: %v", err)
	}

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("leader offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("leader metadata store: %v", err)
	}

	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		ReplicationFactor: 1,
	}, store, offsetStore, metaStore, nil, nil)
	if err != nil {
		t.Fatalf("leader broker init: %v", err)
	}

	server, err := netproto.NewServer("127.0.0.1:0", b)
	if err != nil {
		t.Fatalf("leader server: %v", err)
	}

	ctxSrv, cancelSrv := context.WithCancel(ctx)
	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe(ctxSrv)
	}()

	addr := waitForAddr(t, server).String()
	cleanup := func() {
		cancelSrv()
		assertAsyncError(t, serverErr, context.Canceled)

		closeExpectNoErr(t, "leader broker", b.Close())
		closeExpectNoErr(t, "leader storage", store.Close())
		closeExpectNoErr(t, "leader offset store", offsetStore.Close())
		closeExpectNoErr(t, "leader metadata store", metaStore.Close())
	}

	return addr, cleanup, b
}

func produceValues(ctx context.Context, t *testing.T, b *broker.Broker, topic string, partition int, values []string) {
	t.Helper()

	for _, val := range values {
		if _, err := b.Produce(ctx, topic, partition, []api.Record{{Value: []byte(val)}}); err != nil {
			t.Fatalf("leader produce: %v", err)
		}
	}
}

func readLogValues(ctx context.Context, t *testing.T, store *storage.Manager) []string {
	t.Helper()

	log, err := store.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	defer func() {
		closeExpectNoErr(t, "alpha log", log.Close())
	}()

	recs, err := log.Read(ctx, 0, 1<<20)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}

	res := make([]string, 0, len(recs))
	for _, r := range recs {
		res = append(res, string(r.Value))
	}

	return res
}

func runReplicatorUntil(ctx context.Context, t *testing.T, rep *PartitionReplicator, sink *walSink, target api.Offset) {
	t.Helper()

	ctxRep, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	runErr := make(chan error, 1)

	go func() {
		runErr <- rep.Run(ctxRep)

		close(done)
	}()

	waitUntil(t, func() bool {
		next, err := sink.NextOffset()
		if err != nil || next == 0 {
			return false
		}

		return next > target
	}, 3*time.Second)
	cancel()
	<-done
	assertAsyncError(t, runErr, context.Canceled)
}

func waitUntil(t *testing.T, pred func() bool, timeout time.Duration) {
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
			t.Fatalf("condition not met within %s", timeout)
		case <-ticker.C:
		}
	}
}

func closeExpectNoErr(t *testing.T, target string, err error) {
	t.Helper()

	if err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close %s: %v", target, err)
	}
}
