package controller

import (
	"context"
	"testing"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestSingleNodeControllerBuildsMetadata(t *testing.T) {
	topics := map[string]metadata.TopicState{
		"alpha": {
			Name:              "alpha",
			NumPartitions:     2,
			ReplicationFactor: 1,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 3}}},
				{ID: 1, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 4}}},
			},
		},
		"beta": {
			Name:              "beta",
			NumPartitions:     1,
			ReplicationFactor: 1,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 2}}},
			},
		},
	}
	cfg := api.BrokerConfig{BrokerID: 1, ClusterID: "c1", AdvertisedAddr: "localhost:9999"}
	ctrl := NewSingleNodeController(cfg, topics)
	meta, err := ctrl.GetClusterMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetClusterMetadata: %v", err)
	}
	if meta.Version != 1 {
		t.Fatalf("expected version 1, got %d", meta.Version)
	}
	if len(meta.Brokers) != 1 || meta.Brokers[0].BrokerID != cfg.BrokerID {
		t.Fatalf("unexpected brokers: %+v", meta.Brokers)
	}
	if len(meta.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(meta.Partitions))
	}
	for _, p := range meta.Partitions {
		if p.Leader != cfg.BrokerID {
			t.Fatalf("unexpected leader for %s-%d: %d", p.Topic, p.Partition, p.Leader)
		}
		if len(p.Replicas) != 1 || p.Replicas[0] != cfg.BrokerID {
			t.Fatalf("unexpected replicas for %s-%d: %+v", p.Topic, p.Partition, p.Replicas)
		}
		if len(p.ISR) != 1 || p.ISR[0] != cfg.BrokerID {
			t.Fatalf("unexpected ISR for %s-%d: %+v", p.Topic, p.Partition, p.ISR)
		}
		if p.Topic == "alpha" && p.Partition == 0 && p.LeaderEpoch != 3 {
			t.Fatalf("expected epoch 3 for alpha-0, got %d", p.LeaderEpoch)
		}
	}
}
