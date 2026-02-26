package broker_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	broker "github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/internal/replication"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestRaftReplicationSurvivesRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	data1 := t.TempDir()
	data2 := t.TempDir()
	raftDir1 := t.TempDir()
	raftDir2 := t.TempDir()

	raftAddr1 := freeTCPAddr(t)
	raftAddr2 := freeTCPAddr(t)
	binaryAddr1 := freeTCPAddr(t)
	binaryAddr2 := freeTCPAddr(t)
	peers := []string{raftAddr1, raftAddr2}

	static := &api.StaticClusterConfig{
		ClusterID: "cluster-rf2",
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: binaryAddr1},
			{BrokerID: 2, Host: binaryAddr2},
		},
	}
	cfg1 := api.BrokerConfig{
		BrokerID:          1,
		ControllerMode:    "raft",
		RaftBindAddr:      raftAddr1,
		RaftPeers:         peers,
		RaftDir:           raftDir1,
		StaticCluster:     static,
		BinaryAddr:        binaryAddr1,
		ReplicationFactor: 2,
		Replication:       true,
		DataDir:           data1,
	}
	cfg2 := cfg1
	cfg2.BrokerID = 2
	cfg2.RaftBindAddr = raftAddr2
	cfg2.RaftDir = raftDir2
	cfg2.BinaryAddr = binaryAddr2
	cfg2.DataDir = data2

	initial := api.ClusterMetadata{
		ClusterID: static.ClusterID,
		Version:   1,
		Brokers:   static.Brokers,
	}

	rc1, err := controller.NewRaftController(cfg1, initial, raftDir1)
	if err != nil {
		t.Fatalf("controller1: %v", err)
	}
	defer rc1.Close()

	rc2, err := controller.NewRaftController(cfg2, initial, raftDir2)
	if err != nil {
		t.Fatalf("controller2: %v", err)
	}
	defer rc2.Close()

	ctrls := []*controller.RaftController{rc1, rc2}
	leaderCtrl := waitForLeaderCtrl(t, ctrls)

	// Register brokers and assign topic with RF=2.
	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: binaryAddr1}); err != nil {
		t.Fatalf("register broker1: %v", err)
	}

	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: binaryAddr2}); err != nil {
		t.Fatalf("register broker2: %v", err)
	}

	clusterMeta, err := leaderCtrl.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1, ReplicationFactor: 2})
	if err != nil {
		t.Fatalf("assign topic: %v", err)
	}

	waitForClusterMeta(t, ctrls, 1)

	topicEvent, err := createTopicEventFromMeta(clusterMeta, "alpha")
	if err != nil {
		t.Fatalf("prepare topic event: %v", err)
	}

	nodes := startBrokerNodes(t, ctx, []api.BrokerConfig{cfg1, cfg2}, []string{binaryAddr1, binaryAddr2}, []*controller.RaftController{rc1, rc2}, topicEvent)

	defer func() {
		for _, n := range nodes {
			n.shutdown()
		}
	}()

	leaderID, followerID := leaderFollowerIDs(topicEvent)
	leaderNode := nodes[leaderID]
	followerNode := nodes[followerID]
	waitForBrokerPartitions(t, ctx, leaderNode, followerNode)

	repCancel, repErr := startReplication(ctx, leaderNode, followerNode, followerID)
	defer repCancel()

	sendProduce := makeProduceFunc(t, "alpha")
	sendFetch := makeFetchFunc(t, "alpha")

	if resp, err := sendProduce(leaderNode.cfg.BinaryAddr, []api.Record{{Value: []byte("one")}, {Value: []byte("two")}}); err != nil || resp.Error != api.ErrNone {
		t.Fatalf("produce on leader: resp=%+v err=%v", resp, err)
	}

	awaitReplication(t, ctx, followerNode.store, repErr, 1)
	assertNotLeaderResponses(t, followerNode.cfg.BinaryAddr, sendProduce, sendFetch)

	// Simulate full restart of brokers and controllers.
	repCancel()

	for _, n := range nodes {
		n.shutdown()
	}

	for _, c := range ctrls {
		_ = c.Close()
	}

	// Restart controllers using same Raft state and verify metadata restored.
	rc1, err = controller.NewRaftController(cfg1, api.ClusterMetadata{}, raftDir1)
	if err != nil {
		t.Fatalf("restart controller1: %v", err)
	}
	defer rc1.Close()

	rc2, err = controller.NewRaftController(cfg2, api.ClusterMetadata{}, raftDir2)
	if err != nil {
		t.Fatalf("restart controller2: %v", err)
	}
	defer rc2.Close()

	ctrls = []*controller.RaftController{rc1, rc2}

	leaderCtrl = waitForLeaderCtrl(t, ctrls)
	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: binaryAddr1}); err != nil {
		t.Fatalf("re-register broker1: %v", err)
	}

	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: binaryAddr2}); err != nil {
		t.Fatalf("re-register broker2: %v", err)
	}

	waitForClusterMeta(t, ctrls, 1)

	clusterMeta, err = leaderCtrl.GetClusterMetadata(ctx)
	if err != nil {
		t.Fatalf("metadata after restart: %v", err)
	}

	if len(clusterMeta.Partitions) != 1 || clusterMeta.Partitions[0].Topic != "alpha" {
		t.Fatalf("expected topic metadata after restart, got %+v", clusterMeta.Partitions)
	}

	topicEvent, err = createTopicEventFromMeta(clusterMeta, "alpha")
	if err != nil {
		t.Fatalf("topic event after restart: %v", err)
	}

	nodes = startBrokerNodes(t, ctx, []api.BrokerConfig{cfg1, cfg2}, []string{binaryAddr1, binaryAddr2}, []*controller.RaftController{rc1, rc2}, topicEvent)

	defer func() {
		for _, n := range nodes {
			n.shutdown()
		}
	}()

	leaderID, followerID = leaderFollowerIDs(topicEvent)
	leaderNode = nodes[leaderID]
	followerNode = nodes[followerID]
	waitForBrokerPartitions(t, ctx, leaderNode, followerNode)

	repCancel, repErr = startReplication(ctx, leaderNode, followerNode, followerID)
	defer repCancel()

	if resp, err := sendProduce(leaderNode.cfg.BinaryAddr, []api.Record{{Value: []byte("three")}, {Value: []byte("four")}}); err != nil || resp.Error != api.ErrNone {
		t.Fatalf("produce on leader after restart: resp=%+v err=%v", resp, err)
	}

	awaitReplication(t, ctx, followerNode.store, repErr, 3)
	assertNotLeaderResponses(t, followerNode.cfg.BinaryAddr, sendProduce, sendFetch)
}

type brokerNode struct {
	cfg       api.BrokerConfig
	meta      *metadata.Store
	store     *storage.Manager
	offsets   *broker.OffsetStore
	broker    *broker.Broker
	server    *netproto.Server
	srvCancel context.CancelFunc
	ctrl      *controller.RaftController
}

func (n *brokerNode) shutdown() {
	if n == nil {
		return
	}

	if n.srvCancel != nil {
		n.srvCancel()
	}

	if n.server != nil {
		_ = n.server.Close()
	}

	if n.broker != nil {
		_ = n.broker.Close()
	}

	if n.offsets != nil {
		_ = n.offsets.Close()
	}

	if n.store != nil {
		_ = n.store.Close()
	}

	if n.meta != nil {
		_ = n.meta.Close()
	}
}

func startBrokerNodes(t *testing.T, ctx context.Context, cfgs []api.BrokerConfig, addrs []string, ctrls []*controller.RaftController, topic metadata.CreateTopicEvent) map[int]*brokerNode {
	t.Helper()

	nodes := make(map[int]*brokerNode)

	for idx, cfg := range cfgs {
		meta, err := metadata.NewStore(api.BrokerConfig{DataDir: cfg.DataDir})
		if err != nil {
			t.Fatalf("metadata store: %v", err)
		}

		recovered, err := meta.RecoverTopics(ctx)
		if err != nil {
			t.Fatalf("recover topics: %v", err)
		}

		if len(recovered.Topics) == 0 {
			if err := meta.AppendCreateTopic(ctx, topic); err != nil {
				t.Fatalf("append topic: %v", err)
			}
		}

		store, _ := storage.NewManager(storage.Config{DataDir: cfg.DataDir})
		if err := store.Recover(ctx); err != nil {
			t.Fatalf("store recover: %v", err)
		}

		offsets, _ := broker.NewOffsetStore(cfg.DataDir)
		if offsets == nil {
			t.Fatalf("offset store nil")
		}

		b, err := broker.NewBroker(cfg, store, offsets, meta, ctrls[idx], nil)
		if err != nil {
			t.Fatalf("broker init: %v", err)
		}

		srv, _ := netproto.NewServer(addrs[idx], b)

		srvCtx, srvCancel := context.WithCancel(ctx)

		go func() {
			_ = srv.ListenAndServe(srvCtx)
		}()

		nodes[cfg.BrokerID] = &brokerNode{cfg: cfg, meta: meta, store: store, offsets: offsets, broker: b, server: srv, srvCancel: srvCancel, ctrl: ctrls[idx]}
	}

	return nodes
}

func createTopicEventFromMeta(meta api.ClusterMetadata, topic string) (metadata.CreateTopicEvent, error) {
	ev := metadata.CreateTopicEvent{Name: topic}
	for _, p := range meta.Partitions {
		if p.Topic != topic {
			continue
		}

		part := metadata.PartitionSpec{ID: int32(p.Partition)}
		for _, r := range p.Replicas {
			role := api.RoleFollower
			if r == p.Leader {
				role = api.RoleLeader
			}

			part.Replicas = append(part.Replicas, metadata.ReplicaSpec{BrokerID: int32(r), Role: role, LeaderEpoch: p.LeaderEpoch})
		}

		ev.Partitions = append(ev.Partitions, part)
	}

	if len(ev.Partitions) == 0 {
		return metadata.CreateTopicEvent{}, fmt.Errorf("topic %s not found in metadata", topic)
	}

	ev.NumPartitions = len(ev.Partitions)
	ev.ReplicationFactor = len(ev.Partitions[0].Replicas)

	return ev, nil
}

func startReplication(ctx context.Context, leader *brokerNode, follower *brokerNode, followerID int) (context.CancelFunc, <-chan error) {
	rep := replication.NewBinaryReplicator()
	sink := replication.NewReportingSink(replication.NewWALSink(follower.store, "alpha", 0), follower.ctrl, "alpha", 0, followerID)
	pr := replication.NewPartitionReplicator(rep, api.BrokerInfo{BrokerID: leader.cfg.BrokerID, Host: leader.cfg.BinaryAddr}, "alpha", 0, sink)
	repCtx, cancel := context.WithCancel(ctx)
	repErr := make(chan error, 1)

	go func() {
		repErr <- pr.Run(repCtx)
	}()

	return cancel, repErr
}

func awaitReplication(t *testing.T, ctx context.Context, store *storage.Manager, repErr <-chan error, expectedHWM api.Offset) {
	t.Helper()

	var replicationErr error

	caughtUp := waitUntil(t, func() bool {
		select {
		case err := <-repErr:
			replicationErr = err
			return true
		default:
		}

		log, _ := store.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})

		return log.HighWatermark() >= expectedHWM
	}, 10*time.Second)
	if replicationErr != nil && !errors.Is(replicationErr, context.Canceled) {
		t.Fatalf("replication failed: %v", replicationErr)
	}

	if !caughtUp {
		log, _ := store.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
		t.Fatalf("follower did not catch up, hwm=%d", log.HighWatermark())
	}
}

func assertNotLeaderResponses(t *testing.T, followerAddr string, sendProduce func(string, []api.Record) (*netproto.ProduceResponse, error), sendFetch func(string, api.Offset) (*netproto.FetchResponse, error)) {
	t.Helper()

	prodResp, err := sendProduce(followerAddr, []api.Record{{Value: []byte("forbidden")}})
	if err != nil {
		t.Fatalf("follower produce via netproto: %v", err)
	}

	if prodResp.Error != api.ErrNotLeader {
		t.Fatalf("expect not leader error for follower produce, got %v", prodResp.Error)
	}

	fetchResp, err := sendFetch(followerAddr, 0)
	if err != nil {
		t.Fatalf("follower fetch via netproto: %v", err)
	}

	if fetchResp.Error != api.ErrNotLeader {
		t.Fatalf("expect not leader error for follower fetch, got %v", fetchResp.Error)
	}
}

func waitForBrokerPartitions(t *testing.T, ctx context.Context, nodes ...*brokerNode) {
	t.Helper()

	ok := waitUntil(t, func() bool {
		for _, n := range nodes {
			meta, err := n.broker.Metadata(ctx, []string{"alpha"})
			if err != nil || len(meta) == 0 {
				return false
			}
		}

		return true
	}, 5*time.Second)
	if !ok {
		t.Fatalf("brokers did not load metadata in time")
	}
}

func leaderFollowerIDs(topic metadata.CreateTopicEvent) (int, int) {
	var leader, follower int

	for _, r := range topic.Partitions[0].Replicas {
		if r.Role == api.RoleLeader {
			leader = int(r.BrokerID)
		} else {
			follower = int(r.BrokerID)
		}
	}

	return leader, follower
}

func makeProduceFunc(t *testing.T, topic string) func(string, []api.Record) (*netproto.ProduceResponse, error) {
	t.Helper()

	return func(addr string, records []api.Record) (*netproto.ProduceResponse, error) {
		req := &netproto.ProduceRequest{Topic: topic, Partition: 0, Records: records}

		payload, err := netproto.EncodeProduceRequest(req)
		if err != nil {
			return nil, err
		}

		respPayload, err := sendProtoRequest(addr, api.APIKeyProduce, payload)
		if err != nil {
			return nil, err
		}

		return netproto.DecodeProduceResponse(respPayload)
	}
}

func makeFetchFunc(t *testing.T, topic string) func(string, api.Offset) (*netproto.FetchResponse, error) {
	t.Helper()

	return func(addr string, offset api.Offset) (*netproto.FetchResponse, error) {
		req := &netproto.FetchRequest{Topic: topic, Partition: 0, Offset: offset, MaxBytes: 4096}

		payload, err := netproto.EncodeFetchRequest(req)
		if err != nil {
			return nil, err
		}

		respPayload, err := sendProtoRequest(addr, api.APIKeyFetch, payload)
		if err != nil {
			return nil, err
		}

		return netproto.DecodeFetchResponse(respPayload)
	}
}

func sendProtoRequest(addr string, key api.APIKey, payload []byte) ([]byte, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	frame, err := netproto.EncodeRequestFrame(key, 1, payload)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}

	_, _, respPayload, err := netproto.DecodeResponseFrame(conn)

	return respPayload, err
}
