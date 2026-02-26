package replication

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeController struct {
	calls []struct {
		topic string
		part  int
		bid   int
		last  api.Offset
		hwm   api.Offset
	}
}

func (f *fakeController) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	return api.ClusterMetadata{}, nil
}

func (f *fakeController) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata)
	close(ch)

	return ch, nil
}

func (f *fakeController) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return api.ClusterMetadata{}, nil
}

func (f *fakeController) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx

	f.calls = append(f.calls, struct {
		topic string
		part  int
		bid   int
		last  api.Offset
		hwm   api.Offset
	}{topic: topic, part: partition, bid: brokerID, last: lastOffset, hwm: leaderHighWatermark})

	return api.ClusterMetadata{}, nil
}

func (f *fakeController) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

type stubSink struct {
	last api.Offset
}

func (s *stubSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = ctx
	_ = records
	_ = highWatermark

	return s.last, nil
}

func (s *stubSink) NextOffset() (api.Offset, error) {
	return s.last + 1, nil
}

func TestReportingSinkReportsProgress(t *testing.T) {
	observability.ReplicationApplied.Reset()
	observability.ReplicationLag.Reset()

	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	inner := NewWALSink(store, "alpha", 0)
	ctrl := &fakeController{}
	sink := NewReportingSink(inner, ctrl, "alpha", 0, 2)

	last, err := sink.ApplyBatch(context.Background(), []api.Record{{Value: []byte("x")}}, 5)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if last != 0 {
		t.Fatalf("expected last offset 0, got %d", last)
	}

	if len(ctrl.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(ctrl.calls))
	}

	call := ctrl.calls[0]
	if call.topic != "alpha" || call.part != 0 || call.bid != 2 || call.last != 0 || call.hwm != 5 {
		t.Fatalf("unexpected call: %+v", call)
	}

	log, _ := store.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	_ = log.Close()

	applied := testutil.ToFloat64(observability.ReplicationApplied.WithLabelValues("alpha", "0", "2"))
	if applied != 1 {
		t.Fatalf("expected applied counter 1, got %f", applied)
	}

	lag := testutil.ToFloat64(observability.ReplicationLag.WithLabelValues("alpha", "0", "2"))
	if lag != 5 {
		t.Fatalf("expected lag 5, got %f", lag)
	}
}

func TestReportingSinkUpdatesLagWithoutRecords(t *testing.T) {
	observability.ReplicationApplied.Reset()
	observability.ReplicationLag.Reset()

	sink := NewReportingSink(&stubSink{last: 2}, nil, "beta", 1, 3)

	last, err := sink.ApplyBatch(context.Background(), nil, 7)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if last != 2 {
		t.Fatalf("unexpected last offset %d", last)
	}

	lag := testutil.ToFloat64(observability.ReplicationLag.WithLabelValues("beta", "1", "3"))
	if lag != 5 {
		t.Fatalf("expected lag 5, got %f", lag)
	}

	applied := testutil.ToFloat64(observability.ReplicationApplied.WithLabelValues("beta", "1", "3"))
	if applied != 0 {
		t.Fatalf("expected no applied records, got %f", applied)
	}
}
