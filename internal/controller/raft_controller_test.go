package controller

import (
	"context"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestRaftControllerAssignTopic(t *testing.T) {
	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-raft",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1:1"},
				{BrokerID: 2, Host: "b2:1"},
			},
		},
		ControllerMode: "raft",
	}
	initial := api.ClusterMetadata{
		ClusterID: cfg.StaticCluster.ClusterID,
		Version:   1,
		Brokers:   cfg.StaticCluster.Brokers,
	}
	rc, err := NewRaftController(cfg, initial, "")
	if err != nil {
		t.Fatalf("new raft controller: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	meta, err := rc.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 2})
	if err != nil {
		t.Fatalf("assign topic: %v", err)
	}
	if len(meta.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(meta.Partitions))
	}
	if meta.Version != 2 {
		t.Fatalf("expected version 2, got %d", meta.Version)
	}
	leaders := make(map[int]int)
	for _, p := range meta.Partitions {
		leaders[p.Leader]++
		if len(p.Replicas) != 1 || p.Replicas[0] != p.Leader {
			t.Fatalf("replicas mismatch for %s-%d: %+v", p.Topic, p.Partition, p.Replicas)
		}
	}
	if len(leaders) != 2 {
		t.Fatalf("expected leaders across 2 brokers, got %+v", leaders)
	}
}

func TestRaftControllerReportReplicaProgress(t *testing.T) {
	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-raft",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1:1"},
				{BrokerID: 2, Host: "b2:1"},
			},
		},
		ControllerMode: "raft",
	}
	initial := api.ClusterMetadata{
		ClusterID: cfg.StaticCluster.ClusterID,
		Version:   1,
		Brokers:   cfg.StaticCluster.Brokers,
	}
	rc, err := NewRaftController(cfg, initial, "")
	if err != nil {
		t.Fatalf("new raft controller: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	meta, err := rc.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1})
	if err != nil {
		t.Fatalf("assign topic: %v", err)
	}
	// inject replica 2 into replicas list to simulate RF=2
	rc.fsm.mu.Lock()
	if len(rc.fsm.meta.Partitions) > 0 {
		rc.fsm.meta.Partitions[0].Replicas = []int{1, 2}
		rc.fsm.meta.Partitions[0].ISR = []int{1}
	}
	rc.fsm.mu.Unlock()

	meta2, err := rc.ReportReplicaProgress(ctx, "alpha", 0, 2, 10, 10)
	if err != nil {
		t.Fatalf("report progress: %v", err)
	}
	isr := meta2.Partitions[0].ISR
	if !(contains(isr, 1) && contains(isr, 2)) {
		t.Fatalf("expected ISR to include 1 and 2, got %+v", isr)
	}
	if meta2.Version != meta.Version+1 {
		t.Fatalf("expected version increment, got %d", meta2.Version)
	}
}
