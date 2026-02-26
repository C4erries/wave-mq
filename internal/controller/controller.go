package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/pkg/api"
)

// MetadataStore provides access to cluster metadata snapshots/updates.
type MetadataStore interface {
	GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error)
	WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error)
	RegisterBroker(ctx context.Context, info api.BrokerInfo) error
	AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error)
	ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error)
}

// Controller manages brokers, topics and assignments.
type Controller interface {
	RegisterBroker(ctx context.Context, info api.BrokerInfo) error
	AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error)
	ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error)
}

// SingleNodeController is a placeholder controller for single-node deployments.
// It returns static metadata with a single broker and local partitions recovered from metadata.log.
type SingleNodeController struct {
	mu   sync.RWMutex
	meta api.ClusterMetadata
	cfg  api.BrokerConfig
	pub  metadataPublisher
}

// NewSingleNodeController builds a controller view for a single broker or a static multi-broker cluster using recovered topics.
func NewSingleNodeController(cfg api.BrokerConfig, topics map[string]metadata.TopicState) (*SingleNodeController, error) {
	brokers, clusterID, err := resolveBrokers(cfg)
	if err != nil {
		return nil, err
	}

	partitions := buildAssignments(cfg, brokers, topics)
	meta := api.ClusterMetadata{
		ClusterID:  clusterID,
		Version:    1,
		Brokers:    brokers,
		Partitions: partitions,
	}

	return &SingleNodeController{meta: meta, cfg: cfg}, nil
}

func (c *SingleNodeController) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.meta, nil
}

func (c *SingleNodeController) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	c.mu.RLock()
	meta := c.meta
	c.mu.RUnlock()
	ch := c.pub.watch(ctx, sinceVersion, meta)

	return ch, nil
}

func (c *SingleNodeController) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx

	if c.cfg.StaticCluster == nil && info.BrokerID != c.cfg.BrokerID {
		return fmt.Errorf("single-node controller refuses broker %d (local %d)", info.BrokerID, c.cfg.BrokerID)
	}

	if c.cfg.StaticCluster != nil {
		if !brokerPresent(info.BrokerID, c.cfg.StaticCluster.Brokers) {
			return fmt.Errorf("broker %d not in static cluster", info.BrokerID)
		}
	}
	// TODO: expand to manage multiple brokers.
	return nil
}

func (c *SingleNodeController) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx

	c.mu.Lock()
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}

	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = c.cfg.ReplicationFactor
	}

	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}

	newParts := assignTopicPartitions(c.cfg, c.meta.Brokers, name, cfg.Partitions, cfg.ReplicationFactor, c.meta.Partitions)
	c.meta.Partitions = append(c.meta.Partitions, newParts...)
	c.meta.Version++
	meta := c.meta
	c.mu.Unlock()
	c.pub.publish(meta)

	return meta, nil
}

// ReportReplicaProgress updates ISR based on follower progress relative to leader high watermark.
func (c *SingleNodeController) ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error) {
	_ = ctx

	c.mu.Lock()
	idx := -1

	for i, p := range c.meta.Partitions {
		if p.Topic == topic && p.Partition == partition {
			idx = i
			break
		}
	}

	if idx == -1 {
		meta := c.meta
		c.mu.Unlock()

		return meta, fmt.Errorf("partition not found")
	}

	assign := c.meta.Partitions[idx]
	if !brokerPresent(brokerID, brokersFromInts(assign.Replicas)) {
		meta := c.meta
		c.mu.Unlock()

		return meta, fmt.Errorf("broker %d not in replicas", brokerID)
	}

	shouldBeISR := lastOffset >= leaderHighWatermark
	assign.ISR = ensureLeaderInISR(assign.Leader, assign.ISR)

	inISR := contains(assign.ISR, brokerID)
	switch {
	case shouldBeISR && !inISR:
		assign.ISR = append(assign.ISR, brokerID)
	case !shouldBeISR && inISR && brokerID != assign.Leader:
		assign.ISR = remove(assign.ISR, brokerID)
	}

	c.meta.Partitions[idx] = assign
	c.meta.Version++
	meta := c.meta
	c.mu.Unlock()
	c.pub.publish(meta)

	return meta, nil
}

func buildAssignments(cfg api.BrokerConfig, brokers []api.BrokerInfo, topics map[string]metadata.TopicState) []api.PartitionAssignment {
	var res []api.PartitionAssignment
	if len(brokers) == 0 {
		return res
	}

	sort.Slice(brokers, func(i, j int) bool { return brokers[i].BrokerID < brokers[j].BrokerID })

	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		state := topics[name]
		parts := append([]metadata.PartitionSpec(nil), state.Partitions...)
		sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

		rf := state.ReplicationFactor
		if rf <= 0 {
			rf = cfg.ReplicationFactor
		}
		// TODO: reuse recovered replica layout when multi-broker placement is persisted.
		assignments := assignTopicPartitions(cfg, brokers, name, len(parts), rf, res)
		for i := range assignments {
			assignments[i].Partition = int(parts[i].ID)
			assignments[i].LeaderEpoch = replicaEpoch(assignments[i].Leader, parts[i].Replicas)
		}

		res = append(res, assignments...)
	}

	return res
}

func replicaEpoch(brokerID int, replicas []metadata.ReplicaSpec) int32 {
	for _, r := range replicas {
		if int(r.BrokerID) == brokerID {
			return r.LeaderEpoch
		}
	}
	// fallback: if no matching replica found, try first replica epoch.
	if len(replicas) > 0 {
		return replicas[0].LeaderEpoch
	}

	return 0
}

func resolveBrokers(cfg api.BrokerConfig) ([]api.BrokerInfo, string, error) {
	if cfg.StaticCluster == nil {
		host := cfg.AdvertisedAddr
		if host == "" {
			host = cfg.BinaryAddr
		}

		return []api.BrokerInfo{
			{BrokerID: cfg.BrokerID, Host: host},
		}, cfg.ClusterID, nil
	}

	if len(cfg.StaticCluster.Brokers) == 0 {
		return nil, "", fmt.Errorf("static cluster must list brokers")
	}

	if !brokerPresent(cfg.BrokerID, cfg.StaticCluster.Brokers) {
		return nil, "", fmt.Errorf("local broker %d not in static cluster", cfg.BrokerID)
	}

	clusterID := cfg.StaticCluster.ClusterID
	if clusterID == "" {
		clusterID = cfg.ClusterID
	}

	return cfg.StaticCluster.Brokers, clusterID, nil
}

func brokerPresent(id int, brokers []api.BrokerInfo) bool {
	for _, b := range brokers {
		if b.BrokerID == id {
			return true
		}
	}

	return false
}

func assignTopicPartitions(cfg api.BrokerConfig, brokers []api.BrokerInfo, name string, partitions int, rf int, existing []api.PartitionAssignment) []api.PartitionAssignment {
	if len(brokers) == 0 {
		return nil
	}

	sort.Slice(brokers, func(i, j int) bool { return brokers[i].BrokerID < brokers[j].BrokerID })

	if rf <= 0 {
		rf = cfg.ReplicationFactor
	}

	if partitions < 0 {
		partitions = 0
	}

	if rf < 1 {
		rf = 1
	}

	replicaCount := rf
	if replicaCount > len(brokers) {
		replicaCount = len(brokers)
	}

	counter := len(existing)

	var res []api.PartitionAssignment

	for pid := 0; pid < partitions; pid++ {
		leaderIdx := counter % len(brokers)
		counter++

		replicas := make([]int, 0, replicaCount)
		for i := 0; i < replicaCount; i++ {
			replicas = append(replicas, brokers[(leaderIdx+i)%len(brokers)].BrokerID)
		}

		res = append(res, api.PartitionAssignment{
			Topic:       name,
			Partition:   pid,
			Replicas:    replicas,
			ISR:         append([]int(nil), replicas...),
			Leader:      replicas[0],
			LeaderEpoch: 0,
		})
	}

	return res
}

func brokersFromInts(ids []int) []api.BrokerInfo {
	res := make([]api.BrokerInfo, 0, len(ids))
	for _, id := range ids {
		res = append(res, api.BrokerInfo{BrokerID: id})
	}

	return res
}

func ensureLeaderInISR(leader int, isr []int) []int {
	if contains(isr, leader) {
		return isr
	}

	return append(isr, leader)
}

func contains(list []int, id int) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}

	return false
}

func remove(list []int, id int) []int {
	var res []int

	for _, v := range list {
		if v != id {
			res = append(res, v)
		}
	}

	return res
}
