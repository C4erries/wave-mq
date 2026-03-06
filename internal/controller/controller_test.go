package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestSingleNodeControllerBuildsMetadata(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

func TestReportReplicaProgressAddsToISR(t *testing.T) {
	t.Parallel()

	topics := map[string]metadata.TopicState{
		"alpha": {
			Name:              "alpha",
			NumPartitions:     1,
			ReplicationFactor: 2,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{
					{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1},
					{BrokerID: 2, Role: api.RoleFollower, LeaderEpoch: 1},
				}},
			},
		},
	}
	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-1",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1"},
				{BrokerID: 2, Host: "b2"},
			},
		},
	}

	ctrl, err := NewSingleNodeController(cfg, topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	// Seed ISR to only leader.
	ctrl.meta.Partitions[0].ISR = []int{1}
	ctrl.meta.Partitions[0].Replicas = []int{1, 2}

	meta, err := ctrl.ReportReplicaProgress(context.Background(), "alpha", 0, 2, 10, 10)
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	isr := meta.Partitions[0].ISR
	if !contains(isr, 1) || !contains(isr, 2) {
		t.Fatalf("expected brokers 1 and 2 in ISR, got %+v", isr)
	}

	if meta.Version != 2 {
		t.Fatalf("expected version 2, got %d", meta.Version)
	}
}

func TestReportReplicaProgressRemovesFromISR(t *testing.T) {
	t.Parallel()

	topics := map[string]metadata.TopicState{
		"alpha": {
			Name:              "alpha",
			NumPartitions:     1,
			ReplicationFactor: 2,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{
					{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1},
					{BrokerID: 2, Role: api.RoleFollower, LeaderEpoch: 1},
				}},
			},
		},
	}
	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-1",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1"},
				{BrokerID: 2, Host: "b2"},
			},
		},
	}

	ctrl, err := NewSingleNodeController(cfg, topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	ctrl.meta.Partitions[0].ISR = []int{1, 2}
	ctrl.meta.Partitions[0].Replicas = []int{1, 2}

	meta, err := ctrl.ReportReplicaProgress(context.Background(), "alpha", 0, 2, 5, 10)
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	isr := meta.Partitions[0].ISR
	if contains(isr, 2) {
		t.Fatalf("expected broker 2 to be removed from ISR, got %+v", isr)
	}

	if !contains(isr, 1) {
		t.Fatalf("leader must stay in ISR")
	}
}

func TestReportReplicaProgressRejectsUnknownReplica(t *testing.T) {
	t.Parallel()

	topics := map[string]metadata.TopicState{
		"alpha": {
			Name:              "alpha",
			NumPartitions:     1,
			ReplicationFactor: 1,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{
					{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1},
				}},
			},
		},
	}
	cfg := api.BrokerConfig{BrokerID: 1}

	ctrl, err := NewSingleNodeController(cfg, topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	_, err = ctrl.ReportReplicaProgress(context.Background(), "alpha", 0, 999, 1, 1)
	if err == nil {
		t.Fatalf("expected error for unknown replica")
	}
}

func TestAssignTopicUpdatesMetadata(t *testing.T) {
	t.Parallel()

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

func TestAssignTopicSingleNodeReplicationFactorTruncates(t *testing.T) {
	t.Parallel()

	cfg := api.BrokerConfig{BrokerID: 1, ReplicationFactor: 1}

	ctrl, err := NewSingleNodeController(cfg, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	meta, err := ctrl.AssignTopic(context.Background(), "alpha", api.TopicConfig{Partitions: 2, ReplicationFactor: 3})
	if err != nil {
		t.Fatalf("assign alpha: %v", err)
	}

	for _, p := range meta.Partitions {
		if len(p.Replicas) != 1 || p.Replicas[0] != cfg.BrokerID {
			t.Fatalf("expected single replica on broker %d, got %+v", cfg.BrokerID, p.Replicas)
		}

		if len(p.ISR) != 1 || p.ISR[0] != cfg.BrokerID {
			t.Fatalf("expected single ISR member, got %+v", p.ISR)
		}

		if p.Leader != cfg.BrokerID {
			t.Fatalf("expected leader %d, got %d", cfg.BrokerID, p.Leader)
		}
	}
}

func TestAssignTopicStaticClusterReplicationFactor(t *testing.T) {
	t.Parallel()

	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-rf",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1"},
				{BrokerID: 2, Host: "b2"},
				{BrokerID: 3, Host: "b3"},
			},
		},
	}

	ctrl, err := NewSingleNodeController(cfg, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	meta, err := ctrl.AssignTopic(context.Background(), "alpha", api.TopicConfig{Partitions: 3, ReplicationFactor: 3})
	if err != nil {
		t.Fatalf("assign alpha: %v", err)
	}

	leaders := make(map[int]int)
	for _, p := range meta.Partitions {
		leaders[p.Leader]++
		if len(p.Replicas) != 3 {
			t.Fatalf("expected 3 replicas, got %+v", p.Replicas)
		}

		seen := make(map[int]struct{})
		for _, id := range p.Replicas {
			seen[id] = struct{}{}
		}

		if len(seen) != len(p.Replicas) {
			t.Fatalf("replicas must be unique, got %+v", p.Replicas)
		}

		if p.Leader != p.Replicas[0] {
			t.Fatalf("leader must be first replica, got %d with replicas %+v", p.Leader, p.Replicas)
		}

		if len(p.ISR) != len(p.Replicas) {
			t.Fatalf("ISR should match replicas initially, got %+v", p.ISR)
		}
	}

	if len(leaders) < 2 {
		t.Fatalf("leaders should be distributed, got %+v", leaders)
	}
}

func TestRecoveredAssignmentsUseReplicationFactor(t *testing.T) {
	t.Parallel()

	topics := map[string]metadata.TopicState{
		"alpha": {
			Name:              "alpha",
			NumPartitions:     2,
			ReplicationFactor: 3,
			Partitions: []metadata.PartitionSpec{
				{ID: 0, Replicas: []metadata.ReplicaSpec{
					{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 1},
					{BrokerID: 2, Role: api.RoleFollower, LeaderEpoch: 1},
					{BrokerID: 3, Role: api.RoleFollower, LeaderEpoch: 1},
				}},
				{ID: 1, Replicas: []metadata.ReplicaSpec{
					{BrokerID: 2, Role: api.RoleLeader, LeaderEpoch: 1},
					{BrokerID: 3, Role: api.RoleFollower, LeaderEpoch: 1},
					{BrokerID: 1, Role: api.RoleFollower, LeaderEpoch: 1},
				}},
			},
		},
	}
	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-rf",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1},
				{BrokerID: 2},
				{BrokerID: 3},
			},
		},
	}

	ctrl, err := NewSingleNodeController(cfg, topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	meta, err := ctrl.GetClusterMetadata(context.Background())
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}

	if len(meta.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(meta.Partitions))
	}

	for _, p := range meta.Partitions {
		if len(p.Replicas) != 3 {
			t.Fatalf("expected 3 replicas, got %+v", p.Replicas)
		}

		if p.Leader != p.Replicas[0] {
			t.Fatalf("leader should be first replica, got %d", p.Leader)
		}

		if len(p.ISR) != len(p.Replicas) {
			t.Fatalf("ISR should include all replicas, got %+v", p.ISR)
		}
	}
}

func TestSingleNodeControllerWatchStreamsUpdates(t *testing.T) {
	t.Parallel()

	cfg := api.BrokerConfig{BrokerID: 1, ClusterID: "watch-1"}

	ctrl, err := NewSingleNodeController(cfg, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates, err := ctrl.WatchClusterMetadata(ctx, 0)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	awaitVersion := func(expected int64) api.ClusterMetadata {
		t.Helper()

		select {
		case meta, ok := <-updates:
			if !ok {
				t.Fatalf("channel closed before receiving version %d", expected)
			}

			if meta.Version != expected {
				t.Fatalf("expected version %d, got %d", expected, meta.Version)
			}

			return meta
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for version %d", expected)
		}

		return api.ClusterMetadata{}
	}

	awaitVersion(1)

	if _, err := ctrl.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("assign topic: %v", err)
	}

	awaitVersion(2)

	if _, err := ctrl.ReportReplicaProgress(ctx, "alpha", 0, cfg.BrokerID, 1, 1); err != nil {
		t.Fatalf("report progress: %v", err)
	}

	awaitVersion(3)
}

func TestSingleNodeControllerAssignTopicRejectsDuplicate(t *testing.T) {
	t.Parallel()

	cfg := api.BrokerConfig{BrokerID: 1, ClusterID: "dup-1"}

	ctrl, err := NewSingleNodeController(cfg, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	ctx := context.Background()

	if _, err := ctrl.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("assign alpha first: %v", err)
	}

	_, err = ctrl.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1})
	if !errors.Is(err, ErrTopicExists) {
		t.Fatalf("expected ErrTopicExists, got %v", err)
	}
}

func TestSingleNodeControllerRegisterBrokerUpdatesMetadata(t *testing.T) {
	t.Parallel()

	cfg := api.BrokerConfig{
		BrokerID: 1,
		StaticCluster: &api.StaticClusterConfig{
			ClusterID: "cluster-reg",
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1"},
				{BrokerID: 2, Host: "b2"},
			},
		},
	}

	ctrl, err := NewSingleNodeController(cfg, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	ctx := context.Background()

	initial, err := ctrl.GetClusterMetadata(ctx)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}

	err = ctrl.RegisterBroker(ctx, api.BrokerInfo{
		BrokerID:       2,
		Host:           "b2-new",
		HTTPAddr:       "b2:18090",
		ControllerAddr: "b2:9001",
	})
	if err != nil {
		t.Fatalf("register broker: %v", err)
	}

	updated, err := ctrl.GetClusterMetadata(ctx)
	if err != nil {
		t.Fatalf("get updated metadata: %v", err)
	}

	if updated.Version != initial.Version+1 {
		t.Fatalf("expected version increment from %d to %d, got %d", initial.Version, initial.Version+1, updated.Version)
	}

	var broker2 *api.BrokerInfo

	for i := range updated.Brokers {
		if updated.Brokers[i].BrokerID == 2 {
			broker2 = &updated.Brokers[i]
			break
		}
	}

	if broker2 == nil {
		t.Fatalf("broker 2 not found in metadata: %+v", updated.Brokers)
	}

	if broker2.HTTPAddr != "b2:18090" || broker2.ControllerAddr != "b2:9001" {
		t.Fatalf("broker metadata not updated: %+v", *broker2)
	}
}
