package replication

import (
	"context"
	"fmt"

	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/pkg/api"
)

type reportingSink struct {
	inner    Sink
	ctrl     controller.MetadataStore
	topic    string
	part     int
	brokerID int
}

// NewReportingSink wraps a sink to report progress to the controller.
func NewReportingSink(inner Sink, ctrl controller.MetadataStore, topic string, part int, brokerID int) Sink {
	return &reportingSink{
		inner:    inner,
		ctrl:     ctrl,
		topic:    topic,
		part:     part,
		brokerID: brokerID,
	}
}

func (s *reportingSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	last, err := s.inner.ApplyBatch(ctx, records, highWatermark)
	if err != nil {
		return last, err
	}

	if len(records) > 0 {
		observability.ReplicationApplied.WithLabelValues(s.topic, fmt.Sprintf("%d", s.part), fmt.Sprintf("%d", s.brokerID)).Add(float64(len(records)))
	}

	lag := highWatermark - last
	if lag < 0 {
		lag = 0
	}

	observability.ReplicationLag.WithLabelValues(s.topic, fmt.Sprintf("%d", s.part), fmt.Sprintf("%d", s.brokerID)).Set(float64(lag))

	if s.ctrl != nil {
		if _, err := s.ctrl.ReportReplicaProgress(ctx, s.topic, s.part, s.brokerID, last, highWatermark); err != nil {
			return last, err
		}
	}

	return last, nil
}

// NextOffset delegates to the inner sink if it can provide an offset hint.
func (s *reportingSink) NextOffset() (api.Offset, error) {
	if prov, ok := s.inner.(OffsetProvider); ok {
		return prov.NextOffset()
	}

	return 0, nil
}

// EnsureLeaderHighWatermark delegates to the inner sink when available.
func (s *reportingSink) EnsureLeaderHighWatermark(ctx context.Context, leaderHighWatermark api.Offset) (api.Offset, error) {
	if align, ok := s.inner.(HighWatermarkAligner); ok {
		return align.EnsureLeaderHighWatermark(ctx, leaderHighWatermark)
	}

	if prov, ok := s.inner.(OffsetProvider); ok {
		return prov.NextOffset()
	}

	return 0, nil
}
