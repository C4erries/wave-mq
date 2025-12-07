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
	meta, err := rc.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 2, ReplicationFactor: 2})
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
		if len(p.Replicas) != 2 {
			t.Fatalf("replicas mismatch for %s-%d: %+v", p.Topic, p.Partition, p.Replicas)
		}
		seen := map[int]struct{}{}
		for _, id := range p.Replicas {
			seen[id] = struct{}{}
		}
		if len(seen) != len(p.Replicas) {
			t.Fatalf("replicas must be unique, got %+v", p.Replicas)
		}
		if p.Leader != p.Replicas[0] {
			t.Fatalf("leader should be first replica, got %d", p.Leader)
		}
	}
	if len(leaders) != 2 {
		t.Fatalf("expected leaders across 2 brokers, got %+v", leaders)
	}
	meta, err = rc.AssignTopic(ctx, "beta", api.TopicConfig{Partitions: 1, ReplicationFactor: 3})
	if err != nil {
		t.Fatalf("assign topic beta: %v", err)
	}
	if meta.Version != 3 {
		t.Fatalf("expected version 3 after second assignment, got %d", meta.Version)
	}
	found := false
	for _, p := range meta.Partitions {
		if p.Topic != "beta" {
			continue
		}
		found = true
		if len(p.Replicas) != 2 {
			t.Fatalf("expected replicas truncated to 2 brokers, got %+v", p.Replicas)
		}
		if p.Leader != p.Replicas[0] {
			t.Fatalf("leader should be first replica for beta, got %d", p.Leader)
		}
	}
	if !found {
		t.Fatalf("beta partition not found in metadata")
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

func TestRaftControllerRegisterBrokerSingleNode(t *testing.T) {
	cfg := api.BrokerConfig{
		BrokerID:       1,
		ControllerMode: "raft",
	}
	initial := api.ClusterMetadata{
		ClusterID: "c1",
		Version:   1,
	}
	rc, err := NewRaftController(cfg, initial, "")
	if err != nil {
		t.Fatalf("new raft controller: %v", err)
	}
	defer rc.raft.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rc.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: "b1"}); err != nil {
		t.Fatalf("register broker 1: %v", err)
	}
	meta, _ := rc.GetClusterMetadata(ctx)
	if len(meta.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(meta.Brokers))
	}
	firstVersion := meta.Version

	// idempotent re-register
	if err := rc.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: "b1"}); err != nil {
		t.Fatalf("register broker 1 again: %v", err)
	}
	meta, _ = rc.GetClusterMetadata(ctx)
	if meta.Version != firstVersion {
		t.Fatalf("version should not change on identical re-register: got %d want %d", meta.Version, firstVersion)
	}

	if err := rc.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: "b2"}); err != nil {
		t.Fatalf("register broker 2: %v", err)
	}
	meta, _ = rc.GetClusterMetadata(ctx)
	if len(meta.Brokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(meta.Brokers))
	}
	if meta.Version != firstVersion+1 {
		t.Fatalf("expected version to increment on new broker")
	}
}

func TestRaftControllerRestartsWithPersistentState(t *testing.T) {
	dir := t.TempDir()
	cfg := api.BrokerConfig{BrokerID: 1, ControllerMode: "raft"}
	initial := api.ClusterMetadata{ClusterID: "c1", Version: 1}

	rc, err := NewRaftController(cfg, initial, dir)
	if err != nil {
		t.Fatalf("new raft controller: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	meta, err := rc.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 2})
	if err != nil {
		t.Fatalf("assign topic: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("shutdown raft: %v", err)
	}

	rc, err = NewRaftController(cfg, api.ClusterMetadata{}, dir)
	if err != nil {
		t.Fatalf("restart raft controller: %v", err)
	}
	defer rc.Close()

	if err := rc.waitForLeader(ctx); err != nil {
		t.Fatalf("wait for leader after restart: %v", err)
	}

	if err := waitForMetadata(func() bool {
		restored, _ := rc.GetClusterMetadata(ctx)
		return len(restored.Partitions) == len(meta.Partitions) && restored.Version == meta.Version
	}); err != nil {
		t.Fatalf("metadata not restored: %v", err)
	}
	restored, _ := rc.GetClusterMetadata(ctx)
	if restored.ClusterID != meta.ClusterID {
		t.Fatalf("cluster id mismatch after restart: got %s want %s", restored.ClusterID, meta.ClusterID)
	}
	if len(restored.Partitions) != len(meta.Partitions) {
		t.Fatalf("partition count mismatch after restart: got %d want %d", len(restored.Partitions), len(meta.Partitions))
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

func TestRaftControllerMultiPeerRegisterBrokerReplicates(t *testing.T) {
	addr1 := freeAddr(t)
	addr2 := freeAddr(t)
	peers := []string{addr1, addr2}

	cfg1 := api.BrokerConfig{
		BrokerID:       1,
		ControllerMode: "raft",
		RaftBindAddr:   addr1,
		RaftPeers:      peers,
	}
	cfg2 := cfg1
	cfg2.BrokerID = 2
	cfg2.RaftBindAddr = addr2

	initial := api.ClusterMetadata{
		ClusterID: "cluster-raft",
		Version:   1,
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

	leader := waitForLeader(t, []*RaftController{rc1, rc2})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := leader.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: "b1"}); err != nil {
		t.Fatalf("register broker1: %v", err)
	}
	if err := leader.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: "b2"}); err != nil {
		t.Fatalf("register broker2: %v", err)
	}

	if err := waitForMetadata(func() bool {
		m1, _ := rc1.GetClusterMetadata(ctx)
		m2, _ := rc2.GetClusterMetadata(ctx)
		return len(m1.Brokers) == 2 && len(m2.Brokers) == 2 && m1.Version == m2.Version
	}); err != nil {
		t.Fatalf("brokers not replicated: %v", err)
	}
}

func TestRaftControllerFailoverApplyCommands(t *testing.T) {
	addr1 := freeAddr(t)
	addr2 := freeAddr(t)
	addr3 := freeAddr(t)
	peers := []string{addr1, addr2, addr3}

	static := &api.StaticClusterConfig{
		ClusterID: "cluster-raft",
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
			{BrokerID: 3, Host: "b3"},
		},
	}
	cfgs := []api.BrokerConfig{
		{BrokerID: 1, ControllerMode: "raft", RaftBindAddr: addr1, RaftPeers: peers, StaticCluster: static},
		{BrokerID: 2, ControllerMode: "raft", RaftBindAddr: addr2, RaftPeers: peers, StaticCluster: static},
		{BrokerID: 3, ControllerMode: "raft", RaftBindAddr: addr3, RaftPeers: peers, StaticCluster: static},
	}
	initial := api.ClusterMetadata{
		ClusterID: static.ClusterID,
		Version:   1,
		Brokers:   static.Brokers,
	}

	var ctrls []*RaftController
	for _, cfg := range cfgs {
		rc, err := NewRaftController(cfg, initial, "")
		if err != nil {
			t.Fatalf("controller: %v", err)
		}
		ctrls = append(ctrls, rc)
	}
	defer func() {
		for _, c := range ctrls {
			_ = c.raft.Shutdown()
		}
	}()

	leader := waitForLeader(t, ctrls)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := leader.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("assign alpha: %v", err)
	}
	if err := waitForMetadata(func() bool {
		for _, c := range ctrls {
			m, _ := c.GetClusterMetadata(ctx)
			if len(m.Partitions) != 1 {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("metadata did not converge before failover: %v", err)
	}

	// Simulate leader failure.
	_ = leader.raft.Shutdown()

	var survivors []*RaftController
	for _, c := range ctrls {
		if c != leader {
			survivors = append(survivors, c)
		}
	}
	newLeader := waitForLeader(t, survivors)
	if _, err := newLeader.AssignTopic(ctx, "beta", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("assign beta after failover: %v", err)
	}
	if err := waitForMetadata(func() bool {
		for _, c := range survivors {
			m, _ := c.GetClusterMetadata(ctx)
			if len(m.Partitions) != 2 {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("metadata did not converge after failover: %v", err)
	}
}

func TestRaftControllerWatchStreamsUpdates(t *testing.T) {
	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "raft-watch",
			Brokers:   []api.BrokerInfo{{BrokerID: 1, Host: "b1"}},
		},
		ControllerMode: "raft",
	}
	initial := api.ClusterMetadata{ClusterID: cfg.StaticCluster.ClusterID, Version: 1, Brokers: cfg.StaticCluster.Brokers}
	rc, err := NewRaftController(cfg, initial, "")
	if err != nil {
		t.Fatalf("new raft controller: %v", err)
	}
	defer rc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	updates, err := rc.WatchClusterMetadata(ctx, 0)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	awaitVersion := func(expected int64) {
		t.Helper()
		select {
		case meta, ok := <-updates:
			if !ok {
				t.Fatalf("channel closed before receiving version %d", expected)
			}
			if meta.Version != expected {
				t.Fatalf("expected version %d, got %d", expected, meta.Version)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for version %d", expected)
		}
	}

	awaitVersion(1)
	if _, err := rc.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("assign topic: %v", err)
	}
	awaitVersion(2)
	if _, err := rc.ReportReplicaProgress(ctx, "alpha", 0, cfg.BrokerID, 1, 1); err != nil {
		t.Fatalf("report progress: %v", err)
	}
	awaitVersion(3)
}
