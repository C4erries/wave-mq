package replication

import (
	"context"

	"github.com/c4erries/wave-mq/internal/controller"
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
