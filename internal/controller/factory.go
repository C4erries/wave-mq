package controller

import (
	"fmt"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/pkg/api"
)

// NewController chooses a controller implementation based on cfg.ControllerMode.
// Mode "single" (default) returns SingleNodeController; "raft" returns RaftController.
func NewController(cfg api.BrokerConfig, topics map[string]metadata.TopicState) (MetadataStore, error) {
	mode := cfg.ControllerMode
	if mode == "" {
		mode = "single"
	}
	switch mode {
	case "single":
		return NewSingleNodeController(cfg, topics)
	case "raft":
		brokers, clusterID, err := resolveBrokers(cfg)
		if err != nil {
			return nil, err
		}
		initial := api.ClusterMetadata{
			ClusterID: clusterID,
			Version:   1,
			Brokers:   brokers,
		}
		return NewRaftController(cfg, initial, cfg.RaftDir)
	default:
		return nil, fmt.Errorf("unknown controller mode: %s", cfg.ControllerMode)
	}
}
