package replication

import (
	"context"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeSink struct {
	records       []api.Record
	highWatermark api.Offset
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
