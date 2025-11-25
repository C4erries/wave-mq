package controller

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"

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

func TestRaftControllerMultiPeerAssignTopic(t *testing.T) {
	addr1 := freeAddr(t)
	addr2 := freeAddr(t)
	peers := []string{addr1, addr2}

	baseCluster := &api.StaticClusterConfig{
		ClusterID: "cluster-raft",
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
	}

	cfg1 := api.BrokerConfig{
		BrokerID:       1,
		ControllerMode: "raft",
		RaftBindAddr:   addr1,
		RaftPeers:      peers,
		StaticCluster:  baseCluster,
	}
	cfg2 := cfg1
	cfg2.BrokerID = 2
	cfg2.RaftBindAddr = addr2

	initial := api.ClusterMetadata{
		ClusterID: baseCluster.ClusterID,
		Version:   1,
		Brokers:   baseCluster.Brokers,
	}

	rc1, err := NewRaftController(cfg1, initial, "")
	if err != nil {
		t.Fatalf("new raft controller 1: %v", err)
	}
	defer rc1.raft.Shutdown()
	rc2, err := NewRaftController(cfg2, initial, "")
	if err != nil {
		t.Fatalf("new raft controller 2: %v", err)
	}
	defer rc2.raft.Shutdown()

	ctrls := []*RaftController{rc1, rc2}
	leader := waitForLeader(t, ctrls)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := leader.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 2}); err != nil {
		t.Fatalf("assign topic on leader: %v", err)
	}

	if err := waitForMetadata(func() bool {
		m1, _ := rc1.GetClusterMetadata(ctx)
		m2, _ := rc2.GetClusterMetadata(ctx)
		return len(m1.Partitions) == 2 && len(m2.Partitions) == 2 && m1.Version == m2.Version
	}); err != nil {
		t.Fatalf("metadata not replicated: %v", err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitForLeader(t *testing.T, ctrls []*RaftController) *RaftController {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range ctrls {
			if c.raft.State() == raft.Leader {
				return c
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("leader not elected")
	return nil
}

func waitForMetadata(pred func() bool) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("condition not met before deadline")
}
