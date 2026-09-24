package replication

import (
	"context"
	"sync"

	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type walSink struct {
	mu    sync.Mutex
	store *storage.Manager
	topic string
	part  int

	log storage.Log
}

// NewWALSink returns a sink that appends replicated records to local WAL.
func NewWALSink(store *storage.Manager, topic string, part int) Sink {
	return &walSink{store: store, topic: topic, part: part}
}

// EnsureLeaderHighWatermark ensures the local log is opened and returns the next offset to fetch
// from the leader based on the current high watermark.
func (s *walSink) EnsureLeaderHighWatermark(ctx context.Context, leaderHighWatermark api.Offset) (api.Offset, error) {
	_ = ctx
	_ = leaderHighWatermark

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureLogLocked(); err != nil {
		return -1, err
	}

	return s.log.HighWatermark() + 1, nil
}

func (s *walSink) ensureLogLocked() error {
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureLogLocked(); err != nil {
		return -1, err
	}

	return s.log.HighWatermark() + 1, nil
}

func (s *walSink) ApplyBatch(ctx context.Context, records []api.Record, highWatermark api.Offset) (api.Offset, error) {
	_ = highWatermark

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureLogLocked(); err != nil {
		return -1, err
	}

	base, err := s.log.AppendBatch(ctx, records)
	if err != nil {
		return -1, err
	}

	last := base + api.Offset(len(records)) - 1

	return last, nil
}
