package controller

import (
	"context"

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
// It returns static metadata with a single broker.
type SingleNodeController struct {
	meta api.ClusterMetadata
}

// NewSingleNodeController builds a static controller view for a single broker.
func NewSingleNodeController(cfg api.BrokerConfig) *SingleNodeController {
	host := cfg.AdvertisedAddr
	if host == "" {
		host = cfg.BinaryAddr
	}
	meta := api.ClusterMetadata{
		ClusterID: cfg.ClusterID,
		Version:   1,
		Brokers: []api.BrokerInfo{
			{
				BrokerID: cfg.BrokerID,
				Host:     host,
			},
		},
	}
	return &SingleNodeController{meta: meta}
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
	_ = info
	// TODO: expand to manage multiple brokers.
	return nil
}

func (c *SingleNodeController) AssignTopic(ctx context.Context, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = cfg
	// TODO: assign leaders/replicas and update metadata.
	return c.meta, nil
}
