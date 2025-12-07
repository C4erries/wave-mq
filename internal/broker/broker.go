package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

// Storage abstracts the log manager used by the broker.
type Storage interface {
	OpenLog(opts storage.LogOptions) (storage.Log, error)
}

var (
	ErrTopicExists       = errors.New("topic already exists")
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrNotLeader         = errors.New("not leader for partition")
)

// NotLeaderError conveys leader info to callers when hitting a follower.
type NotLeaderError struct {
	Topic     string
	Partition int
	Leader    int
}

func (e NotLeaderError) Error() string {
	if e.Leader != 0 {
		return fmt.Sprintf("not leader for %s/%d, leader=%d", e.Topic, e.Partition, e.Leader)
	}
	return fmt.Sprintf("not leader for %s/%d", e.Topic, e.Partition)
}

func (e NotLeaderError) Is(target error) bool {
	return target == ErrNotLeader
}

// Broker owns topics, partitions, consumer groups and access to local storage.
type Broker struct {
	cfg     api.BrokerConfig
	storage Storage
	offsets *OffsetStore
	meta    *metadata.Store
	cluster controller.MetadataStore

	mu     sync.RWMutex
	topics map[string]*Topic
	groups map[string]*ConsumerGroup
	known  map[string]metadata.TopicState
	closed bool

	commitCount int
}

// Topic represents a logical stream of ordered partitions.
type Topic struct {
	Name              string
	Partitions        map[int]*Partition
	ReplicationFactor int
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

	assignments map[string]map[string][]int // topic -> memberID -> partitions
	mu          sync.RWMutex
}

// GroupMember describes a single consumer in a group.
type GroupMember struct {
	ClientID string
	Topics   []string
	// Assigned partitions per topic for this member.
	Assignments map[string][]int
}

// NewBroker wires together configuration and the storage backend.
func NewBroker(cfg api.BrokerConfig, storage Storage, offsets *OffsetStore, meta *metadata.Store, cluster controller.MetadataStore) (*Broker, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage is required")
	}
	if offsets == nil {
		return nil, fmt.Errorf("offset store is required")
	}
	if meta == nil {
		return nil, fmt.Errorf("metadata store is required")
	}
	if cfg.ReplicationFactor < 1 {
		return nil, fmt.Errorf("replication factor must be >= 1")
	}
	b := &Broker{
		cfg:     cfg,
		storage: storage,
		offsets: offsets,
		meta:    meta,
		cluster: cluster,
		topics:  make(map[string]*Topic),
		groups:  make(map[string]*ConsumerGroup),
		known:   make(map[string]metadata.TopicState),
	}
	// Recover committed offsets
	offsetData, err := offsets.Recover(context.Background())
	if err != nil {
		return nil, err
	}
	for group, topics := range offsetData {
		g := &ConsumerGroup{
			Name:        group,
			Members:     make(map[string]*GroupMember),
			Offsets:     make(map[string]map[int]api.Offset),
			assignments: make(map[string]map[string][]int),
		}
		for topic, parts := range topics {
			if _, ok := g.Offsets[topic]; !ok {
				g.Offsets[topic] = make(map[int]api.Offset)
			}
			for pid, off := range parts {
				g.Offsets[topic][pid] = off
			}
		}
		b.groups[group] = g
	}
	recoveredTopics, err := meta.RecoverTopics(context.Background())
	if err != nil {
		return nil, err
	}
	for name, state := range recoveredTopics.Topics {
		b.known[name] = state
	}
	if err := b.bootstrapTopicsFromMetadata(context.Background(), recoveredTopics.Topics); err != nil {
		return nil, err
	}
	return b, nil
}

// CreateTopic initializes partition metadata and local logs.
func (b *Broker) CreateTopic(ctx context.Context, name string, cfg api.TopicConfig) error {
	if name == "" {
		return fmt.Errorf("topic name is required")
	}
	partitions := cfg.Partitions
	if partitions <= 0 {
		partitions = 1
	}
	rf := cfg.ReplicationFactor
	if rf <= 0 {
		rf = b.cfg.ReplicationFactor
	}
	if rf <= 0 {
		rf = 1
	}
	state := metadata.TopicState{
		Name:              name,
		NumPartitions:     partitions,
		ReplicationFactor: rf,
		Partitions:        make([]metadata.PartitionSpec, 0, partitions),
	}
	for p := 0; p < partitions; p++ {
		replicas := make([]metadata.ReplicaSpec, 0, rf)
		for i := 0; i < rf; i++ {
			role := api.RoleLeader
			if i > 0 {
				role = api.RoleFollower
			}
			replicas = append(replicas, metadata.ReplicaSpec{
				BrokerID:    int32(b.cfg.BrokerID),
				Role:        role,
				LeaderEpoch: 0,
			})
		}
		state.Partitions = append(state.Partitions, metadata.PartitionSpec{
			ID:       int32(p),
			Replicas: replicas,
		})
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return ErrTopicExists
	}
	if _, ok := b.known[name]; ok {
		return ErrTopicExists
	}
	event := metadata.CreateTopicEvent{
		Name:              state.Name,
		NumPartitions:     state.NumPartitions,
		ReplicationFactor: state.ReplicationFactor,
		Partitions:        state.Partitions,
	}
	if err := b.meta.AppendCreateTopic(ctx, event); err != nil {
		return err
	}
	b.known[name] = state
	return b.loadTopicLocked(ctx, state, nil)
}

func (b *Broker) bootstrapTopicsFromMetadata(ctx context.Context, topics map[string]metadata.TopicState) error {
	var allowed map[string]map[int]api.PartitionAssignment
	if b.cluster != nil {
		meta, err := b.cluster.GetClusterMetadata(ctx)
		if err != nil {
			return err
		}
		allowed = make(map[string]map[int]api.PartitionAssignment)
		for _, p := range meta.Partitions {
			if p.Leader != b.cfg.BrokerID && !containsInt(p.Replicas, b.cfg.BrokerID) {
				continue
			}
			if _, ok := allowed[p.Topic]; !ok {
				allowed[p.Topic] = make(map[int]api.PartitionAssignment)
			}
			allowed[p.Topic][p.Partition] = p
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, state := range topics {
		if allowed != nil {
			topicParts := allowed[state.Name]
			if len(topicParts) == 0 {
				continue
			}
			parts := make([]metadata.PartitionSpec, 0, len(topicParts))
			for _, ps := range state.Partitions {
				if _, ok := topicParts[int(ps.ID)]; ok {
					parts = append(parts, ps)
				}
			}
			if len(parts) == 0 {
				continue
			}
			state.Partitions = parts
			state.NumPartitions = len(parts)
		}
		var assignments map[int]api.PartitionAssignment
		if allowed != nil {
			assignments = allowed[state.Name]
		}
		if err := b.loadTopicLocked(ctx, state, assignments); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) loadTopicLocked(ctx context.Context, state metadata.TopicState, assignments map[int]api.PartitionAssignment) error {
	if _, ok := b.topics[state.Name]; ok {
		return nil
	}
	topic := &Topic{
		Name:              state.Name,
		ReplicationFactor: state.ReplicationFactor,
		Partitions:        make(map[int]*Partition, state.NumPartitions),
	}
	specs := append([]metadata.PartitionSpec(nil), state.Partitions...)
	sort.Slice(specs, func(i, j int) bool { return specs[i].ID < specs[j].ID })
	for _, ps := range specs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		log, err := b.storage.OpenLog(storage.LogOptions{
			Topic:     state.Name,
			Partition: int(ps.ID),
		})
		if err != nil {
			return err
		}
		assign := api.PartitionAssignment{}
		if assignments != nil {
			assign = assignments[int(ps.ID)]
		}
		replica := b.replicaForPartition(ps)
		role := replica.Role
		epoch := replica.LeaderEpoch
		if assignments != nil {
			if assign.Leader == b.cfg.BrokerID {
				role = api.RoleLeader
			} else if containsInt(assign.Replicas, b.cfg.BrokerID) {
				role = api.RoleFollower
			}
			if assign.LeaderEpoch != 0 {
				epoch = assign.LeaderEpoch
			}
			replica.BrokerID = int32(b.cfg.BrokerID)
		}
		leader := int(replica.BrokerID)
		replicas := []int{int(replica.BrokerID)}
		isr := []int{int(replica.BrokerID)}
		if assignments != nil {
			if assign.Leader != 0 {
				leader = assign.Leader
			}
			if len(assign.Replicas) > 0 {
				replicas = append([]int(nil), assign.Replicas...)
			}
			if len(assign.ISR) > 0 {
				isr = append([]int(nil), assign.ISR...)
			}
		}
		meta := api.PartitionMetadata{
			Replica: api.PartitionReplica{
				Topic:       state.Name,
				Partition:   int(ps.ID),
				BrokerID:    int(replica.BrokerID),
				Role:        role,
				LeaderEpoch: epoch,
			},
			StartOffset:   log.StartOffset(),
			HighWatermark: log.HighWatermark(),
			Leader:        leader,
			Replicas:      replicas,
			ISR:           isr,
		}
		topic.Partitions[int(ps.ID)] = &Partition{
			Metadata: meta,
			Log:      log,
		}
	}
	b.topics[state.Name] = topic
	return nil
}

func (b *Broker) replicaForPartition(ps metadata.PartitionSpec) metadata.ReplicaSpec {
	for _, r := range ps.Replicas {
		if int(r.BrokerID) == b.cfg.BrokerID {
			return r
		}
	}
	if len(ps.Replicas) > 0 {
		return ps.Replicas[0]
	}
	return metadata.ReplicaSpec{
		BrokerID:    int32(b.cfg.BrokerID),
		Role:        api.RoleLeader,
		LeaderEpoch: 0,
	}
}

func containsInt(list []int, id int) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

// Produce appends records to the specified partition.
func (b *Broker) Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error) {
	start := time.Now()
	labels := []string{topic, fmt.Sprintf("%d", partition)}
	defer func(start time.Time) {
		observability.ProduceLatency.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	}(start)
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		observability.RequestErrors.WithLabelValues("broker", "produce").Inc()
		return -1, fmt.Errorf("broker closed")
	}
	t, ok := b.topics[topic]
	if !ok {
		b.mu.RUnlock()
		observability.RequestErrors.WithLabelValues("broker", "produce").Inc()
		return -1, fmt.Errorf("%w", ErrTopicNotFound)
	}
	p, ok := t.Partitions[partition]
	b.mu.RUnlock()
	if !ok {
		observability.RequestErrors.WithLabelValues("broker", "produce").Inc()
		return -1, fmt.Errorf("%w", ErrPartitionNotFound)
	}
	if b.isClustered() && p.Metadata.Replica.Role != api.RoleLeader {
		leader := b.leaderFor(topic, partition)
		observability.RequestErrors.WithLabelValues("broker", "produce").Inc()
		return -1, NotLeaderError{Topic: topic, Partition: partition, Leader: leader}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(records) == 0 {
		return p.Metadata.HighWatermark + 1, nil
	}
	base, err := p.Log.AppendBatch(ctx, records)
	if err != nil {
		observability.RequestErrors.WithLabelValues("broker", "produce").Inc()
		return -1, err
	}
	observability.MessagesProduced.WithLabelValues(labels...).Add(float64(len(records)))
	hw := base + api.Offset(len(records)-1)
	if hw > p.Metadata.HighWatermark {
		p.Metadata.HighWatermark = hw
	}
	return base, nil
}

// Fetch reads records starting from offset for the given partition.
func (b *Broker) Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	start := time.Now()
	labels := []string{topic, fmt.Sprintf("%d", partition)}
	var count int
	defer func() {
		observability.FetchLatency.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
		if count > 0 {
			observability.MessagesConsumed.WithLabelValues(labels...).Add(float64(count))
		}
	}()
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		observability.RequestErrors.WithLabelValues("broker", "fetch").Inc()
		return nil, fmt.Errorf("broker closed")
	}
	t, ok := b.topics[topic]
	if !ok {
		b.mu.RUnlock()
		observability.RequestErrors.WithLabelValues("broker", "fetch").Inc()
		return nil, fmt.Errorf("%w", ErrTopicNotFound)
	}
	p, ok := t.Partitions[partition]
	b.mu.RUnlock()
	if !ok {
		observability.RequestErrors.WithLabelValues("broker", "fetch").Inc()
		return nil, fmt.Errorf("%w", ErrPartitionNotFound)
	}
	if b.isClustered() && p.Metadata.Replica.Role != api.RoleLeader {
		leader := b.leaderFor(topic, partition)
		observability.RequestErrors.WithLabelValues("broker", "fetch").Inc()
		return nil, NotLeaderError{Topic: topic, Partition: partition, Leader: leader}
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if offset > p.Metadata.HighWatermark {
		return []api.Record{}, nil
	}
	recs, err := p.Log.Read(ctx, offset, maxBytes)
	if err != nil {
		observability.RequestErrors.WithLabelValues("broker", "fetch").Inc()
		return nil, err
	}
	count = len(recs)
	return recs, nil
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
		return -1, -1, fmt.Errorf("%w", ErrTopicNotFound)
	}
	p, ok := t.Partitions[partition]
	b.mu.RUnlock()
	if !ok {
		return -1, -1, fmt.Errorf("%w", ErrPartitionNotFound)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	earliest := p.Log.StartOffset()
	latest := p.Log.HighWatermark()
	return earliest, latest, nil
}

// CommitOffset stores a consumer group's committed offset.
func (b *Broker) CommitOffset(ctx context.Context, group string, topic string, partition int, offset api.Offset) error {
	_ = ctx
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[group]
	if !ok {
		g = &ConsumerGroup{
			Name:        group,
			Members:     make(map[string]*GroupMember),
			Offsets:     make(map[string]map[int]api.Offset),
			assignments: make(map[string]map[string][]int),
		}
		b.groups[group] = g
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.Offsets[topic]; !ok {
		g.Offsets[topic] = make(map[int]api.Offset)
	}
	current, ok := g.Offsets[topic][partition]
	if ok && offset < current {
		return fmt.Errorf("offset regression: current=%d new=%d", current, offset)
	}
	// Persist first to keep in-memory consistent with disk.
	if err := b.offsets.AppendCommit(ctx, group, topic, partition, offset); err != nil {
		return err
	}
	g.Offsets[topic][partition] = offset
	b.commitCount++
	if b.commitCount%1000 == 0 {
		snapshot := b.snapshotOffsetsLocked()
		go b.offsets.Compact(context.Background(), snapshot)
	}
	return nil
}

// FetchCommitted returns the last committed offset for a consumer group.
func (b *Broker) FetchCommitted(ctx context.Context, group string, topic string, partition int) (api.Offset, error) {
	_ = ctx
	b.mu.RLock()
	g, ok := b.groups[group]
	b.mu.RUnlock()
	if !ok {
		return -1, fmt.Errorf("group not found")
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	topicOffsets, ok := g.Offsets[topic]
	if !ok {
		return -1, fmt.Errorf("topic not found")
	}
	off, ok := topicOffsets[partition]
	if !ok {
		return -1, fmt.Errorf("partition not found")
	}
	return off, nil
}

// Metadata exposes the current topic/partition layout.
func (b *Broker) Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error) {
	assignments, err := b.clusterAssignments(ctx)
	if err != nil {
		return nil, err
	}
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
		topicAssignments := assignments[name]
		for pid, p := range topic.Partitions {
			assign, hasAssign := topicAssignments[pid]
			// Missing assignments fallback to local view; otherwise include follower metadata too for routing.
			role := p.Metadata.Replica.Role
			leader := int(p.Metadata.Replica.BrokerID)
			replicas := []int{b.cfg.BrokerID}
			isr := []int{b.cfg.BrokerID}
			epoch := p.Metadata.Replica.LeaderEpoch
			if hasAssign {
				leader = assign.Leader
				replicas = append([]int(nil), assign.Replicas...)
				isr = append([]int(nil), assign.ISR...)
				if assign.Leader == b.cfg.BrokerID {
					role = api.RoleLeader
				} else {
					role = api.RoleFollower
				}
				if assign.LeaderEpoch != 0 {
					epoch = assign.LeaderEpoch
				}
			}
			p.mu.RLock()
			meta := api.PartitionMetadata{
				Replica: api.PartitionReplica{
					Topic:       name,
					Partition:   pid,
					BrokerID:    b.cfg.BrokerID,
					Role:        role,
					LeaderEpoch: epoch,
				},
				StartOffset:   p.Log.StartOffset(),
				HighWatermark: p.Log.HighWatermark(),
				Leader:        leader,
				Replicas:      replicas,
				ISR:           isr,
			}
			p.mu.RUnlock()
			res = append(res, meta)
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
	if b.offsets != nil {
		_ = b.offsets.Close()
	}
	if b.meta != nil {
		_ = b.meta.Close()
	}
	return nil
}

func (b *Broker) snapshotOffsetsLocked() map[string]map[string]map[int]api.Offset {
	out := make(map[string]map[string]map[int]api.Offset)
	for group, g := range b.groups {
		g.mu.RLock()
		topics := make(map[string]map[int]api.Offset)
		for topic, parts := range g.Offsets {
			cp := make(map[int]api.Offset, len(parts))
			for pid, off := range parts {
				cp[pid] = off
			}
			topics[topic] = cp
		}
		g.mu.RUnlock()
		out[group] = topics
	}
	return out
}

func (b *Broker) leaderFor(topic string, partition int) int {
	if b.cluster == nil {
		return 0
	}
	meta, err := b.cluster.GetClusterMetadata(context.Background())
	if err != nil {
		return 0
	}
	for _, p := range meta.Partitions {
		if p.Topic == topic && p.Partition == partition {
			return p.Leader
		}
	}
	return 0
}

func (b *Broker) clusterAssignments(ctx context.Context) (map[string]map[int]api.PartitionAssignment, error) {
	if b.cluster == nil {
		return nil, nil
	}
	meta, err := b.cluster.GetClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	assignments := make(map[string]map[int]api.PartitionAssignment)
	for _, p := range meta.Partitions {
		if _, ok := assignments[p.Topic]; !ok {
			assignments[p.Topic] = make(map[int]api.PartitionAssignment)
		}
		assignments[p.Topic][p.Partition] = p
	}
	return assignments, nil
}

func (b *Broker) isClustered() bool {
	return b.cluster != nil
}

// TopicAndPartitionCounts returns counts for summary.
func (b *Broker) TopicAndPartitionCounts() (int, int) {
	assignments, _ := b.clusterAssignments(context.Background())
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := 0
	partitions := 0
	for _, t := range b.topics {
		leaderParts := 0
		topicAssignments := assignments[t.Name]
		for pid, p := range t.Partitions {
			if topicAssignments != nil {
				if assign, ok := topicAssignments[pid]; ok {
					if assign.Leader != b.cfg.BrokerID {
						continue
					}
				}
			}
			if !b.isClustered() || p.Metadata.Replica.Role == api.RoleLeader {
				leaderParts++
			}
		}
		if leaderParts > 0 || !b.isClustered() {
			topics++
		}
		partitions += leaderParts
	}
	return topics, partitions
}

// LocalPartitionsSnapshot returns partition assignments this broker should serve.
// If no cluster metadata provider is configured, it derives assignments from local topics.
func (b *Broker) LocalPartitionsSnapshot(ctx context.Context) ([]api.PartitionAssignment, error) {
	if b.cluster == nil {
		b.mu.RLock()
		defer b.mu.RUnlock()
		var res []api.PartitionAssignment
		for name, t := range b.topics {
			for pid := range t.Partitions {
				res = append(res, api.PartitionAssignment{
					Topic:       name,
					Partition:   pid,
					Replicas:    []int{b.cfg.BrokerID},
					ISR:         []int{b.cfg.BrokerID},
					Leader:      b.cfg.BrokerID,
					LeaderEpoch: 0,
				})
			}
		}
		return res, nil
	}
	meta, err := b.cluster.GetClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	var res []api.PartitionAssignment
	for _, p := range meta.Partitions {
		if p.Leader == b.cfg.BrokerID {
			res = append(res, p)
		}
	}
	return res, nil
}

type TopicSummary struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
}

type PartitionInfo struct {
	ID            int        `json:"id"`
	Leader        int        `json:"leader"`
	HighWatermark api.Offset `json:"highWatermark"`
	StartOffset   api.Offset `json:"startOffset"`
	Replicas      []int      `json:"replicas"`
}

type TopicDetail struct {
	Name              string          `json:"name"`
	PartitionCount    int             `json:"partitionCount"`
	ReplicationFactor int             `json:"replicationFactor"`
	Partitions        []PartitionInfo `json:"partitions"`
}

func (b *Broker) TopicsSnapshot() []TopicSummary {
	assignments, _ := b.clusterAssignments(context.Background())
	b.mu.RLock()
	defer b.mu.RUnlock()
	var res []TopicSummary
	for _, t := range b.topics {
		leaderParts := 0
		topicAssignments := assignments[t.Name]
		for pid, p := range t.Partitions {
			if topicAssignments != nil {
				if assign, ok := topicAssignments[pid]; ok && assign.Leader != b.cfg.BrokerID {
					continue
				}
			}
			if !b.isClustered() || p.Metadata.Replica.Role == api.RoleLeader {
				leaderParts++
			}
		}
		if b.isClustered() && leaderParts == 0 {
			continue
		}
		res = append(res, TopicSummary{
			Name:              t.Name,
			Partitions:        leaderParts,
			ReplicationFactor: t.ReplicationFactor,
		})
	}
	return res
}

func (b *Broker) TopicDetail(name string) (TopicDetail, bool) {
	assignments := b.partitionAssignments(name)
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	if !ok {
		return TopicDetail{}, false
	}
	var parts []PartitionInfo
	for pid, p := range t.Partitions {
		assign, hasAssign := assignments[pid]
		if b.isClustered() {
			if hasAssign && assign.Leader != b.cfg.BrokerID {
				continue
			}
			if !hasAssign && p.Metadata.Replica.Role != api.RoleLeader {
				continue
			}
		}
		p.mu.RLock()
		info := PartitionInfo{
			ID:            pid,
			Leader:        p.Metadata.Replica.BrokerID,
			HighWatermark: p.Log.HighWatermark(),
			StartOffset:   p.Log.StartOffset(),
			Replicas:      []int{p.Metadata.Replica.BrokerID},
		}
		if hasAssign && len(assign.Replicas) > 0 {
			info.Leader = assign.Leader
			info.Replicas = append([]int(nil), assign.Replicas...)
		}
		p.mu.RUnlock()
		parts = append(parts, info)
	}
	return TopicDetail{
		Name:              t.Name,
		PartitionCount:    len(parts),
		ReplicationFactor: t.ReplicationFactor,
		Partitions:        parts,
	}, true
}

func (b *Broker) partitionAssignments(topic string) map[int]api.PartitionAssignment {
	assignments, err := b.clusterAssignments(context.Background())
	if err != nil {
		return nil
	}
	return assignments[topic]
}

// FetchMessages fetches up to limit messages ending at offset (if provided) from a partition.
func (b *Broker) FetchMessages(ctx context.Context, topic string, partition int, offsetParam string, limit int) (api.Offset, []api.Record, error) {
	earliest, latest, err := b.ListOffsets(ctx, topic, partition)
	if err != nil {
		return 0, nil, err
	}
	target := latest
	if offsetParam != "" {
		if v, err := strconv.ParseInt(offsetParam, 10, 64); err == nil {
			target = api.Offset(v)
		} else {
			return 0, nil, fmt.Errorf("invalid offset")
		}
	}
	if target < earliest {
		target = earliest
	}
	start := target - api.Offset(limit) + 1
	if start < earliest {
		start = earliest
	}
	if start < 0 {
		start = 0
	}
	recs, err := b.Fetch(ctx, topic, partition, start, 10<<20)
	if err != nil {
		return 0, nil, err
	}
	var filtered []api.Record
	for _, r := range recs {
		if r.Offset > target {
			continue
		}
		if r.Offset < start {
			continue
		}
		filtered = append(filtered, r)
	}
	// keep only last limit
	if len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	return start, filtered, nil
}

type ConsumerAssignment struct {
	Topic           string     `json:"topic"`
	Partition       int        `json:"partition"`
	CommittedOffset api.Offset `json:"committedOffset"`
	HighWatermark   api.Offset `json:"highWatermark"`
	Lag             api.Offset `json:"lag"`
}

type ConsumerGroupInfo struct {
	Name        string               `json:"name"`
	Members     int                  `json:"members"`
	Assignments []ConsumerAssignment `json:"assignments"`
}

func (b *Broker) ConsumerGroupsSnapshot(ctx context.Context) []ConsumerGroupInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	meta, _ := b.Metadata(ctx, nil)
	hwm := make(map[string]map[int]api.Offset)
	for _, m := range meta {
		if _, ok := hwm[m.Replica.Topic]; !ok {
			hwm[m.Replica.Topic] = make(map[int]api.Offset)
		}
		hwm[m.Replica.Topic][m.Replica.Partition] = m.HighWatermark
	}
	var groups []ConsumerGroupInfo
	for name, g := range b.groups {
		g.mu.RLock()
		info := ConsumerGroupInfo{
			Name:    name,
			Members: len(g.Members),
		}
		for topic, parts := range g.Offsets {
			for pid, off := range parts {
				h := hwm[topic][pid]
				lag := api.Offset(0)
				if h >= 0 && h > off {
					lag = h - off - 1
					if lag < 0 {
						lag = 0
					}
				}
				info.Assignments = append(info.Assignments, ConsumerAssignment{
					Topic:           topic,
					Partition:       pid,
					CommittedOffset: off,
					HighWatermark:   h,
					Lag:             lag,
				})
			}
		}
		g.mu.RUnlock()
		groups = append(groups, info)
	}
	return groups
}

// JoinGroup registers a member in a consumer group and returns its assignments.
// Assignments are calculated per topic using simple round-robin over partitions.
func (b *Broker) JoinGroup(ctx context.Context, group, memberID string, topics []string) (map[string][]int, error) {
	_ = ctx
	if memberID == "" {
		return nil, fmt.Errorf("memberID required")
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("at least one topic required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[group]
	if !ok {
		g = &ConsumerGroup{
			Name:        group,
			Members:     make(map[string]*GroupMember),
			Offsets:     make(map[string]map[int]api.Offset),
			assignments: make(map[string]map[string][]int),
		}
		b.groups[group] = g
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	member, ok := g.Members[memberID]
	if !ok {
		member = &GroupMember{ClientID: memberID, Topics: topics, Assignments: make(map[string][]int)}
		g.Members[memberID] = member
	} else {
		member.Topics = topics
	}
	for _, topic := range topics {
		t, exists := b.topics[topic]
		if !exists {
			return nil, fmt.Errorf("topic not found")
		}
		assignment := rebalanceTopic(t, g)
		g.assignments[topic] = assignment
		updateMemberAssignments(g, topic)
	}
	return member.Assignments, nil
}

// LeaveGroup removes a member and rebalances assignments.
func (b *Broker) LeaveGroup(ctx context.Context, group, memberID string) error {
	_ = ctx
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[group]
	if !ok {
		return fmt.Errorf("group not found")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.Members, memberID)
	for topic, members := range g.assignments {
		delete(members, memberID)
		topicMeta, ok := b.topics[topic]
		if !ok {
			continue
		}
		g.assignments[topic] = rebalanceTopic(topicMeta, g)
		updateMemberAssignments(g, topic)
	}
	// If no members remain, clean up group assignments (group kept for offsets).
	if len(g.Members) == 0 {
		g.assignments = make(map[string]map[string][]int)
	}
	return nil
}

func rebalanceTopic(topic *Topic, g *ConsumerGroup) map[string][]int {
	memberIDs := sortedMembers(g)
	assign := make(map[string][]int)
	if len(memberIDs) == 0 {
		return assign
	}
	parts := sortedPartitions(topic)
	i := 0
	for _, pid := range parts {
		member := memberIDs[i%len(memberIDs)]
		assign[member] = append(assign[member], pid)
		i++
	}
	return assign
}

func updateMemberAssignments(g *ConsumerGroup, topic string) {
	for id, m := range g.Members {
		if m.Assignments == nil {
			m.Assignments = make(map[string][]int)
		}
		if ass, ok := g.assignments[topic]; ok {
			m.Assignments[topic] = ass[id]
		} else {
			delete(m.Assignments, topic)
		}
	}
}

func sortedMembers(g *ConsumerGroup) []string {
	res := make([]string, 0, len(g.Members))
	for id := range g.Members {
		res = append(res, id)
	}
	sort.Strings(res)
	return res
}

func sortedPartitions(t *Topic) []int {
	res := make([]int, 0, len(t.Partitions))
	for p := range t.Partitions {
		res = append(res, p)
	}
	sort.Ints(res)
	return res
}
