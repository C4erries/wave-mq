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
	AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error)
}

// Controller manages brokers, topics and assignments.
type Controller interface {
	RegisterBroker(ctx context.Context, info api.BrokerInfo) error
	AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error)
}

// SingleNodeController is a placeholder controller for single-node deployments.
// It returns static metadata with a single broker and local partitions recovered from metadata.log.
type SingleNodeController struct {
	mu   sync.RWMutex
	meta api.ClusterMetadata
	cfg  api.BrokerConfig
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
	_ = sinceVersion
	ch := make(chan api.ClusterMetadata, 1)
	go func() {
		defer close(ch)
		c.mu.RLock()
		meta := c.meta
		c.mu.RUnlock()
		select {
		case <-ctx.Done():
			return
		case ch <- meta:
		}
	}()
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
	defer c.mu.Unlock()
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}
	newParts := assignTopicPartitions(c.cfg, c.meta.Brokers, name, cfg.Partitions, c.meta.Partitions)
	c.meta.Partitions = append(c.meta.Partitions, newParts...)
	c.meta.Version++
	return c.meta, nil
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
	var counter int
	for _, name := range names {
		state := topics[name]
		parts := append([]metadata.PartitionSpec(nil), state.Partitions...)
		sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })
		for _, ps := range parts {
			leader := brokers[counter%len(brokers)].BrokerID
			counter++
			epoch := replicaEpoch(leader, ps.Replicas)
			res = append(res, api.PartitionAssignment{
				Topic:       name,
				Partition:   int(ps.ID),
				Replicas:    []int{leader},
				ISR:         []int{leader},
				Leader:      leader,
				LeaderEpoch: epoch,
			})
		}
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

func assignTopicPartitions(cfg api.BrokerConfig, brokers []api.BrokerInfo, name string, partitions int, existing []api.PartitionAssignment) []api.PartitionAssignment {
	if len(brokers) == 0 {
		return nil
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].BrokerID < brokers[j].BrokerID })
	counter := len(existing)
	var res []api.PartitionAssignment
	for pid := 0; pid < partitions; pid++ {
		leader := brokers[counter%len(brokers)].BrokerID
		counter++
		res = append(res, api.PartitionAssignment{
			Topic:       name,
			Partition:   pid,
			Replicas:    []int{leader},
			ISR:         []int{leader},
			Leader:      leader,
			LeaderEpoch: 0,
		})
	}
	return res
}
