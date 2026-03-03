package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestMultiBrokerIntegrationWithRaftController(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	// Prepare metadata logs with two partitions owned by different brokers.
	meta1, err := metadata.NewStore(api.BrokerConfig{DataDir: dir1})
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}

	meta2, err := metadata.NewStore(api.BrokerConfig{DataDir: dir2})
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}

	ev := metadata.CreateTopicEvent{
		Name:              "alpha",
		NumPartitions:     2,
		ReplicationFactor: 1,
		Partitions: []metadata.PartitionSpec{
			{ID: 0, Replicas: []metadata.ReplicaSpec{{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 0}}},
			{ID: 1, Replicas: []metadata.ReplicaSpec{{BrokerID: 2, Role: api.RoleLeader, LeaderEpoch: 0}}},
		},
	}
	if err := meta1.AppendCreateTopic(context.Background(), ev); err != nil {
		t.Fatalf("meta1 append: %v", err)
	}

	if err := meta2.AppendCreateTopic(context.Background(), ev); err != nil {
		t.Fatalf("meta2 append: %v", err)
	}

	defer meta1.Close()
	defer meta2.Close()

	addr1 := freeTCPAddr(t)
	addr2 := freeTCPAddr(t)
	peers := []string{addr1, addr2}

	static := &api.StaticClusterConfig{
		ClusterID: "cluster-raft",
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
	}
	cfg1 := api.BrokerConfig{
		BrokerID:          1,
		ControllerMode:    "raft",
		RaftBindAddr:      addr1,
		RaftPeers:         peers,
		StaticCluster:     static,
		ReplicationFactor: 1,
	}
	cfg2 := cfg1
	cfg2.BrokerID = 2
	cfg2.RaftBindAddr = addr2

	initial := api.ClusterMetadata{
		ClusterID: static.ClusterID,
		Version:   1,
		Brokers:   static.Brokers,
	}

	rc1, err := controller.NewRaftController(cfg1, initial, "")
	if err != nil {
		t.Fatalf("controller1: %v", err)
	}
	defer rc1.Close()

	rc2, err := controller.NewRaftController(cfg2, initial, "")
	if err != nil {
		t.Fatalf("controller2: %v", err)
	}
	defer rc2.Close()

	ctrls := []*controller.RaftController{rc1, rc2}
	leader := waitForLeaderCtrl(t, ctrls)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := leader.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: "b1"}); err != nil {
		t.Fatalf("register broker1: %v", err)
	}

	if err := leader.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: "b2"}); err != nil {
		t.Fatalf("register broker2: %v", err)
	}

	if _, err := leader.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 2}); err != nil {
		t.Fatalf("assign topic: %v", err)
	}

	waitForClusterMeta(t, ctrls, 2)

	metaAfter, _ := rc1.GetClusterMetadata(ctx)

	leaders := map[int]struct{}{}
	for _, p := range metaAfter.Partitions {
		leaders[p.Leader] = struct{}{}
	}

	if len(leaders) < 2 {
		t.Fatalf("expected leaders across both brokers, got %+v", leaders)
	}

	store1, err := storage.NewManager(storage.Config{DataDir: dir1})
	if err != nil {
		t.Fatalf("store1: %v", err)
	}
	defer store1.Close()

	store2, err := storage.NewManager(storage.Config{DataDir: dir2})
	if err != nil {
		t.Fatalf("store2: %v", err)
	}
	defer store2.Close()

	offset1, err := NewOffsetStore(dir1)
	if err != nil {
		t.Fatalf("offset1: %v", err)
	}
	defer offset1.Close()

	offset2, err := NewOffsetStore(dir2)
	if err != nil {
		t.Fatalf("offset2: %v", err)
	}
	defer offset2.Close()

	b1, err := NewBroker(cfg1, store1, offset1, meta1, rc1, nil)
	if err != nil {
		t.Fatalf("broker1: %v", err)
	}
	defer b1.Close()

	b2, err := NewBroker(cfg2, store2, offset2, meta2, rc2, nil)
	if err != nil {
		t.Fatalf("broker2: %v", err)
	}
	defer b2.Close()

	checkPartitions := func(b *Broker, expected int, pid int) {
		b.mu.RLock()
		topic := b.topics["alpha"]
		b.mu.RUnlock()

		if topic == nil {
			t.Fatalf("topic missing")
		}

		if len(topic.Partitions) != expected {
			t.Fatalf("expected %d partitions, got %d", expected, len(topic.Partitions))
		}

		if _, ok := topic.Partitions[pid]; !ok {
			t.Fatalf("expected partition %d to exist", pid)
		}
	}

	checkPartitions(b1, 1, 0)
	checkPartitions(b2, 1, 1)

	assign1, err := b1.LocalPartitionsSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot1: %v", err)
	}

	assign2, err := b2.LocalPartitionsSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot2: %v", err)
	}

	if len(assign1) != 1 || assign1[0].Leader != 1 || assign1[0].Partition != 0 {
		t.Fatalf("unexpected assignments on b1: %+v", assign1)
	}

	if len(assign2) != 1 || assign2[0].Leader != 2 || assign2[0].Partition != 1 {
		t.Fatalf("unexpected assignments on b2: %+v", assign2)
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := l.Addr().String()
	_ = l.Close()

	return addr
}

func waitForLeaderCtrl(t *testing.T, ctrls []*controller.RaftController) *controller.RaftController {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		for _, c := range ctrls {
			if c.RaftState() == raft.Leader.String() {
				return c
			}
		}

		select {
		case <-timer.C:
			t.Fatalf("leader not elected")
		case <-ticker.C:
		}
	}
}

func waitForClusterMeta(t *testing.T, ctrls []*controller.RaftController, parts int) {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		ok := true

		for _, c := range ctrls {
			meta, _ := c.GetClusterMetadata(context.Background())
			if len(meta.Partitions) != parts {
				ok = false
				break
			}
		}

		if ok {
			return
		}

		select {
		case <-timer.C:
			t.Fatalf("metadata did not converge")
		case <-ticker.C:
		}
	}
}
