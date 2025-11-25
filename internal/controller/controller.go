package controller

import (
	"context"
	"fmt"
	"sort"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/pkg/api"
)

// MetadataStore provides access to cluster metadata snapshots/updates.
type MetadataStore interface {
	GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error)
	WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error)
}

// Controller manages brokers, topics and assignments.
type Controller interface {
	RegisterBroker(ctx context.Context, info api.BrokerInfo) error
	AssignTopic(ctx context.Context, cfg api.TopicConfig) (api.ClusterMetadata, error)
}

// SingleNodeController is a placeholder controller for single-node deployments.
// It returns static metadata with a single broker and local partitions recovered from metadata.log.
type SingleNodeController struct {
	meta api.ClusterMetadata
	cfg  api.BrokerConfig
}

// NewSingleNodeController builds a static controller view for a single broker using recovered topics.
func NewSingleNodeController(cfg api.BrokerConfig, topics map[string]metadata.TopicState) *SingleNodeController {
	host := cfg.AdvertisedAddr
	if host == "" {
		host = cfg.BinaryAddr
	}
	partitions := buildAssignments(cfg, topics)
	meta := api.ClusterMetadata{
		ClusterID: cfg.ClusterID,
		Version:   1,
		Brokers: []api.BrokerInfo{
			{
				BrokerID: cfg.BrokerID,
				Host:     host,
			},
		},
		Partitions: partitions,
	}
	return &SingleNodeController{meta: meta, cfg: cfg}
}

func (c *SingleNodeController) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx
	return c.meta, nil
}

func (c *SingleNodeController) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = sinceVersion
	ch := make(chan api.ClusterMetadata, 1)
	go func() {
		defer close(ch)
		select {
		case <-ctx.Done():
			return
		case ch <- c.meta:
		}
	}()
	return ch, nil
}

func (c *SingleNodeController) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	if info.BrokerID != c.cfg.BrokerID {
		return fmt.Errorf("single-node controller refuses broker %d (local %d)", info.BrokerID, c.cfg.BrokerID)
	}
	// TODO: expand to manage multiple brokers.
	return nil
}

func (c *SingleNodeController) AssignTopic(ctx context.Context, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = cfg
	// TODO: assign leaders/replicas and update metadata.
	return c.meta, nil
}

func buildAssignments(cfg api.BrokerConfig, topics map[string]metadata.TopicState) []api.PartitionAssignment {
	var res []api.PartitionAssignment
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		state := topics[name]
		parts := append([]metadata.PartitionSpec(nil), state.Partitions...)
		sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })
		for _, ps := range parts {
			epoch := replicaEpoch(cfg.BrokerID, ps.Replicas)
			res = append(res, api.PartitionAssignment{
				Topic:       name,
				Partition:   int(ps.ID),
				Replicas:    []int{cfg.BrokerID},
				ISR:         []int{cfg.BrokerID},
				Leader:      cfg.BrokerID,
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
