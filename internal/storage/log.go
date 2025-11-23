package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

// Config controls global storage behaviour such as segment sizing and retention.
type Config struct {
	DataDir         string
	MaxSegmentBytes int64
	IndexInterval   int
	SegmentMaxAge   time.Duration
	SyncOnAppend    bool
}

// LogOptions configures a single partition log instance.
type LogOptions struct {
	BaseOffset api.Offset
	Topic      string
	Partition  int
}

// Log represents an append-only segmented commit log for a single partition.
type Log interface {
	Append(ctx context.Context, record api.Record) (api.Offset, error)
	AppendBatch(ctx context.Context, records []api.Record) (api.Offset, error)
	Read(ctx context.Context, offset api.Offset, maxBytes int32) ([]api.Record, error)
	Truncate(ctx context.Context, offset api.Offset) error
	HighWatermark() api.Offset
	Close() error
}

// Manager owns log instances across topics and partitions.
type Manager struct {
	cfg Config
}

// NewManager prepares storage manager state without touching disk yet.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir required")
	}
	return &Manager{cfg: cfg}, nil
}

// OpenLog opens or creates a segmented log for the given topic/partition.
func (m *Manager) OpenLog(opts LogOptions) (Log, error) {
	_ = filepath.Join(m.cfg.DataDir, opts.Topic)
	// TODO: initialize WAL and index files, perform recovery when required.
	return &segmentedLog{opts: opts, cfg: m.cfg}, nil
}

// Recover scans on-disk logs and repairs truncated tails if needed.
func (m *Manager) Recover(ctx context.Context) error {
	// TODO: implement crash recovery by validating segment CRCs and indexes.
	_ = ctx
	return nil
}

// Close flushes any open logs owned by the manager.
func (m *Manager) Close() error {
	return nil
}

// segmentedLog is a placeholder implementation for a partition log.
type segmentedLog struct {
	opts LogOptions
	cfg  Config
}

// Append writes a single record to the WAL.
func (l *segmentedLog) Append(ctx context.Context, record api.Record) (api.Offset, error) {
	_ = ctx
	_ = record
	panic("not implemented")
}

// AppendBatch writes multiple records atomically to preserve ordering.
func (l *segmentedLog) AppendBatch(ctx context.Context, records []api.Record) (api.Offset, error) {
	_ = ctx
	_ = records
	panic("not implemented")
}

// Read returns records starting from the given offset up to maxBytes.
func (l *segmentedLog) Read(ctx context.Context, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	_ = ctx
	_ = offset
	_ = maxBytes
	panic("not implemented")
}

// Truncate discards log data starting from offset (used for recovery).
func (l *segmentedLog) Truncate(ctx context.Context, offset api.Offset) error {
	_ = ctx
	_ = offset
	panic("not implemented")
}

// HighWatermark exposes the durable offset boundary for the partition.
func (l *segmentedLog) HighWatermark() api.Offset {
	panic("not implemented")
}

// Close flushes and releases file handles for the log.
func (l *segmentedLog) Close() error {
	panic("not implemented")
}
