package replication

import (
	"context"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

// Sink consumes replicated records and high watermark.
type Sink interface {
	// ApplyBatch applies records and returns the last applied offset.
	ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error)
}

// OffsetProvider optionally exposes a starting offset hint for replication.
type OffsetProvider interface {
	NextOffset() (api.Offset, error)
}

// PartitionReplicator pulls records for a single partition from its leader and applies them to a sink.
type PartitionReplicator struct {
	rep    Replicator
	leader api.BrokerInfo
	topic  string
	part   int

	sink Sink

	Interval time.Duration
	MaxBytes int32

	nextOffset api.Offset
}

// NewPartitionReplicator constructs a partition replicator with defaults.
func NewPartitionReplicator(rep Replicator, leader api.BrokerInfo, topic string, part int, sink Sink) *PartitionReplicator {
	return &PartitionReplicator{
		rep:        rep,
		leader:     leader,
		topic:      topic,
		part:       part,
		sink:       sink,
		Interval:   50 * time.Millisecond,
		MaxBytes:   1 << 20,
		nextOffset: 0,
	}
}

// Run starts the replication loop until the context is cancelled or an error occurs.
func (p *PartitionReplicator) Run(ctx context.Context) error {
	if p.rep == nil {
		return nil
	}
	if p.nextOffset == 0 {
		if prov, ok := p.sink.(OffsetProvider); ok {
			if off, err := prov.NextOffset(); err == nil {
				p.nextOffset = off
			} else {
				return err
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		fetchResp, err := p.rep.FetchFromLeader(ctx, p.leader, FetchRequest{
			Topic:     p.topic,
			Partition: p.part,
			Offset:    p.nextOffset,
			MaxBytes:  p.MaxBytes,
		})
		if err != nil {
			return err
		}
		if len(fetchResp.Records) == 0 {
			if p.Interval > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(p.Interval):
				}
			}
			continue
		}
		lastApplied, err := p.sink.ApplyBatch(ctx, fetchResp.Records, fetchResp.HighWatermark)
		if err != nil {
			return err
		}
		p.nextOffset = lastApplied + 1
	}
}
