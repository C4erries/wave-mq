package replication

import (
	"context"

	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type walSink struct {
	store *storage.Manager
	topic string
	part  int

	log storage.Log
}

// NewWALSink returns a sink that appends replicated records to local WAL.
func NewWALSink(store *storage.Manager, topic string, part int) Sink {
	return &walSink{store: store, topic: topic, part: part}
}

func (s *walSink) ensureLog() error {
	if s.log != nil {
		return nil
	}
	log, err := s.store.OpenLog(storage.LogOptions{
		Topic:     s.topic,
		Partition: s.part,
	})
	if err != nil {
		return err
	}
	s.log = log
	return nil
}

// NextOffset returns the next offset to replicate into (high watermark + 1).
func (s *walSink) NextOffset() (api.Offset, error) {
	if err := s.ensureLog(); err != nil {
		return -1, err
	}
	return s.log.HighWatermark() + 1, nil
}

func (s *walSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = highWatermark
	if err := s.ensureLog(); err != nil {
		return -1, err
	}
	base, err := s.log.AppendBatch(ctx, records)
	if err != nil {
		return -1, err
	}
	last := base + api.Offset(len(records)) - 1
	return last, nil
}
