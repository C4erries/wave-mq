package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

// Storage abstracts the log manager used by the broker.
type Storage interface {
	OpenLog(opts storage.LogOptions) (storage.Log, error)
}

// Broker owns topics, partitions, consumer groups and access to local storage.
type Broker struct {
	cfg     api.BrokerConfig
	storage Storage

	mu     sync.RWMutex
	topics map[string]*Topic
	groups map[string]*ConsumerGroup
	closed bool
}

// Topic represents a logical stream of ordered partitions.
type Topic struct {
	Name       string
	Partitions map[int]*Partition
}

// Partition holds metadata and a handle to the underlying storage log.
type Partition struct {
	Metadata api.PartitionMetadata
	Log      storage.Log

	mu sync.RWMutex
}

// ConsumerGroup tracks members and committed offsets per topic/partition.
type ConsumerGroup struct {
	Name    string
	Members map[string]*GroupMember
	Offsets map[string]map[int]api.Offset
}

// GroupMember describes a single consumer in a group.
type GroupMember struct {
	ClientID string
	Topics   []string
}

// NewBroker wires together configuration and the storage backend.
func NewBroker(cfg api.BrokerConfig, storage Storage) (*Broker, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage is required")
	}
	if cfg.ReplicationFactor < 1 {
		return nil, fmt.Errorf("replication factor must be >= 1")
	}
	return &Broker{
		cfg:     cfg,
		storage: storage,
		topics:  make(map[string]*Topic),
		groups:  make(map[string]*ConsumerGroup),
	}, nil
}

// CreateTopic initializes partition metadata and local logs.
func (b *Broker) CreateTopic(ctx context.Context, name string, cfg api.TopicConfig) error {
	if name == "" {
		return fmt.Errorf("topic name is required")
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = b.cfg.ReplicationFactor
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return nil
	}

	topic := &Topic{
		Name:       name,
		Partitions: make(map[int]*Partition, cfg.Partitions),
	}

	for p := 0; p < cfg.Partitions; p++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		log, err := b.storage.OpenLog(storage.LogOptions{
			Topic:     name,
			Partition: p,
		})
		if err != nil {
			return err
		}
		meta := api.PartitionMetadata{
			Replica: api.PartitionReplica{
				Topic:       name,
				Partition:   p,
				BrokerID:    b.cfg.BrokerID,
				Role:        api.RoleLeader,
				LeaderEpoch: 0,
			},
			StartOffset:   0,
			HighWatermark: -1,
		}
		topic.Partitions[p] = &Partition{
			Metadata: meta,
			Log:      log,
		}
	}

	b.topics[name] = topic
	return nil
}

// Produce appends records to the specified partition.
func (b *Broker) Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return -1, fmt.Errorf("broker closed")
	}
	t, ok := b.topics[topic]
	if !ok {
		b.mu.RUnlock()
		return -1, fmt.Errorf("topic not found")
	}
	p, ok := t.Partitions[partition]
	b.mu.RUnlock()
	if !ok {
		return -1, fmt.Errorf("partition not found")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(records) == 0 {
		return p.Metadata.HighWatermark + 1, nil
	}
	base, err := p.Log.AppendBatch(ctx, records)
	if err != nil {
		return -1, err
	}
	hw := base + api.Offset(len(records)-1)
	if hw > p.Metadata.HighWatermark {
		p.Metadata.HighWatermark = hw
	}
	return base, nil
}

// Fetch reads records starting from offset for the given partition.
func (b *Broker) Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, fmt.Errorf("broker closed")
	}
	t, ok := b.topics[topic]
	if !ok {
		b.mu.RUnlock()
		return nil, fmt.Errorf("topic not found")
	}
	p, ok := t.Partitions[partition]
	b.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("partition not found")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if offset > p.Metadata.HighWatermark {
		return []api.Record{}, nil
	}
	return p.Log.Read(ctx, offset, maxBytes)
}

// ListOffsets returns offsets such as earliest/latest per partition.
func (b *Broker) ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error) {
	_ = ctx
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return -1, -1, fmt.Errorf("broker closed")
	}
	t, ok := b.topics[topic]
	if !ok {
		b.mu.RUnlock()
		return -1, -1, fmt.Errorf("topic not found")
	}
	p, ok := t.Partitions[partition]
	b.mu.RUnlock()
	if !ok {
		return -1, -1, fmt.Errorf("partition not found")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	earliest := p.Metadata.StartOffset // TODO: retention may advance this.
	latest := p.Metadata.HighWatermark
	return earliest, latest, nil
}

// CommitOffset stores a consumer group's committed offset.
func (b *Broker) CommitOffset(ctx context.Context, group string, topic string, partition int, offset api.Offset) error {
	_ = ctx
	_ = group
	_ = topic
	_ = partition
	_ = offset
	panic("not implemented")
}

// FetchCommitted returns the last committed offset for a consumer group.
func (b *Broker) FetchCommitted(ctx context.Context, group string, topic string, partition int) (api.Offset, error) {
	_ = ctx
	_ = group
	_ = topic
	_ = partition
	panic("not implemented")
}

// Metadata exposes the current topic/partition layout.
func (b *Broker) Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error) {
	_ = ctx
	b.mu.RLock()
	defer b.mu.RUnlock()
	var res []api.PartitionMetadata
	includeAll := len(topics) == 0
	filter := make(map[string]struct{})
	for _, t := range topics {
		filter[t] = struct{}{}
	}
	for name, topic := range b.topics {
		if !includeAll {
			if _, ok := filter[name]; !ok {
				continue
			}
		}
		for _, p := range topic.Partitions {
			p.mu.RLock()
			meta := p.Metadata
			hw := p.Log.HighWatermark()
			if hw > meta.HighWatermark {
				meta.HighWatermark = hw
			}
			res = append(res, meta)
			p.mu.RUnlock()
		}
	}
	// Missing topics are ignored intentionally to allow partial metadata fetch.
	return res, nil
}

// Close shuts down broker resources.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}
