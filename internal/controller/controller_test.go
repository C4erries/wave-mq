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
	ctrl, err := NewSingleNodeController(cfg, topics)
	if err != nil {
		t.Fatalf("NewSingleNodeController: %v", err)
	}
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

func TestStaticClusterAssignmentsRoundRobin(t *testing.T) {
	topics := map[string]metadata.TopicState{
		"alpha": {
			Name:              "alpha",
			NumPartitions:     3,
			ReplicationFactor: 1,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1}}},
				{ID: 1, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1}}},
				{ID: 2, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1}}},
			},
		},
		"beta": {
			Name:              "beta",
			NumPartitions:     1,
			ReplicationFactor: 1,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1}}},
			},
		},
	}
	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-1",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "broker1:7912"},
				{BrokerID: 2, Host: "broker2:7912"},
			},
		},
	}
	ctrl, err := NewSingleNodeController(cfg, topics)
	if err != nil {
		t.Fatalf("NewSingleNodeController: %v", err)
	}
	meta, err := ctrl.GetClusterMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetClusterMetadata: %v", err)
	}
	if meta.ClusterID != "cluster-1" {
		t.Fatalf("unexpected cluster id %s", meta.ClusterID)
	}
	if len(meta.Brokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(meta.Brokers))
	}
	if len(meta.Partitions) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(meta.Partitions))
	}
	leaders := make(map[int]int)
	for _, p := range meta.Partitions {
		leaders[p.Leader]++
		if len(p.Replicas) != 1 || p.Replicas[0] != p.Leader {
			t.Fatalf("replicas mismatch for %s-%d: %+v", p.Topic, p.Partition, p.Replicas)
		}
		if len(p.ISR) != 1 || p.ISR[0] != p.Leader {
			t.Fatalf("isr mismatch for %s-%d: %+v", p.Topic, p.Partition, p.ISR)
		}
	}
	if leaders[1] == 0 || leaders[2] == 0 {
		t.Fatalf("leaders not balanced across brokers: %+v", leaders)
	}
}

func TestAssignTopicUpdatesMetadata(t *testing.T) {
	cfg := api.BrokerConfig{BrokerID: 1, ClusterID: "assign-1"}
	ctrl, err := NewSingleNodeController(cfg, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	ctx := context.Background()
	meta1, err := ctrl.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 2})
	if err != nil {
		t.Fatalf("assign alpha: %v", err)
	}
	if len(meta1.Partitions) != 2 {
		t.Fatalf("expected 2 partitions after alpha, got %d", len(meta1.Partitions))
	}
	if meta1.Version != 2 {
		t.Fatalf("expected version 2 after first assign, got %d", meta1.Version)
	}
	meta2, err := ctrl.AssignTopic(ctx, "beta", api.TopicConfig{Partitions: 1})
	if err != nil {
		t.Fatalf("assign beta: %v", err)
	}
	if len(meta2.Partitions) != 3 {
		t.Fatalf("expected 3 partitions after beta, got %d", len(meta2.Partitions))
	}
	foundAlpha := false
	foundBeta := false
	for _, p := range meta2.Partitions {
		if p.Topic == "alpha" {
			foundAlpha = true
		}
		if p.Topic == "beta" && p.Partition == 0 && p.Leader != cfg.BrokerID {
			t.Fatalf("unexpected leader for beta-0: %d", p.Leader)
		}
		if p.Topic == "beta" {
			foundBeta = true
		}
	}
	if !foundAlpha || !foundBeta {
		t.Fatalf("missing topics in metadata: alpha=%v beta=%v", foundAlpha, foundBeta)
	}
}
