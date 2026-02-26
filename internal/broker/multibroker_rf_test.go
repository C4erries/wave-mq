package broker_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	broker "github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/internal/replication"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestReplicationRF2EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir1 := t.TempDir()
	dir2 := t.TempDir()

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
		StaticCluster:     static,
		BinaryAddr:        binaryAddr1,
		ReplicationFactor: 2,
		Replication:       true,
	}
	cfg2 := cfg1
	cfg2.BrokerID = 2
	cfg2.RaftBindAddr = raftAddr2
	cfg2.BinaryAddr = binaryAddr2

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
	leaderCtrl := waitForLeaderCtrl(t, ctrls)
	// Register brokers and assign topic with RF=2.
	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: binaryAddr1}); err != nil {
		t.Fatalf("register broker1: %v", err)
	}

	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: binaryAddr2}); err != nil {
		t.Fatalf("register broker2: %v", err)
	}

	if _, err := leaderCtrl.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatalf("assign topic: %v", err)
	}

	waitForClusterMeta(t, ctrls, 1)

	// Prepare metadata logs.
	ev := metadata.CreateTopicEvent{
		Name:              "alpha",
		NumPartitions:     1,
		ReplicationFactor: 2,
		Partitions: []metadata.PartitionSpec{
			{ID: 0, Replicas: []metadata.ReplicaSpec{
				{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 0},
				{BrokerID: 2, Role: api.RoleFollower, LeaderEpoch: 0},
			}},
		},
	}

	meta1, _ := metadata.NewStore(api.BrokerConfig{DataDir: dir1})
	defer meta1.Close()

	meta2, _ := metadata.NewStore(api.BrokerConfig{DataDir: dir2})
	defer meta2.Close()

	if err := meta1.AppendCreateTopic(ctx, ev); err != nil {
		t.Fatalf("meta1 append: %v", err)
	}

	if err := meta2.AppendCreateTopic(ctx, ev); err != nil {
		t.Fatalf("meta2 append: %v", err)
	}

	store1, _ := storage.NewManager(storage.Config{DataDir: dir1})
	defer store1.Close()

	store2, _ := storage.NewManager(storage.Config{DataDir: dir2})
	defer store2.Close()

	if err := store1.Recover(ctx); err != nil {
		t.Fatalf("store1 recover: %v", err)
	}

	if err := store2.Recover(ctx); err != nil {
		t.Fatalf("store2 recover: %v", err)
	}

	offset1, _ := broker.NewOffsetStore(dir1)
	offset2, _ := broker.NewOffsetStore(dir2)

	defer offset1.Close()
	defer offset2.Close()

	metaTopics1, _ := meta1.RecoverTopics(ctx)
	metaTopics2, _ := meta2.RecoverTopics(ctx)

	b1, err := broker.NewBroker(cfg1, store1, offset1, meta1, rc1, nil)
	if err != nil {
		t.Fatalf("broker1: %v", err)
	}
	defer b1.Close()

	b2, err := broker.NewBroker(cfg2, store2, offset2, meta2, rc2, nil)
	if err != nil {
		t.Fatalf("broker2: %v", err)
	}
	defer b2.Close()

	// Ensure topics loaded.
	if len(metaTopics1.Topics) == 0 || len(metaTopics2.Topics) == 0 {
		t.Fatalf("topics not recovered")
	}

	// Start servers to serve replication fetches.
	srv1, _ := netproto.NewServer(cfg1.BinaryAddr, b1)
	srv2, _ := netproto.NewServer(cfg2.BinaryAddr, b2)

	ctxSrv, cancelSrv := context.WithCancel(ctx)
	defer cancelSrv()
	srv1Err := make(chan error, 1)
	srv2Err := make(chan error, 1)

	go func() {
		srv1Err <- srv1.ListenAndServe(ctxSrv)
	}()
	go func() {
		srv2Err <- srv2.ListenAndServe(ctxSrv)
	}()
	waitForServerReady(t, cfg1.BinaryAddr, srv1Err)
	waitForServerReady(t, cfg2.BinaryAddr, srv2Err)

	meta, _ := rc1.GetClusterMetadata(ctx)

	var leaderID int

	for _, p := range meta.Partitions {
		if p.Topic == "alpha" && p.Partition == 0 {
			leaderID = p.Leader
		}
	}

	if leaderID == 0 {
		t.Fatalf("leader not found")
	}

	rep := replication.NewBinaryReplicator()
	leaderBroker := b1
	followerBroker := b2
	leaderStore := store1
	followerStore := store2
	followerID := cfg2.BrokerID
	followerCtrl := rc2

	if leaderID == 2 {
		leaderBroker, followerBroker = b2, b1
		leaderStore, followerStore = store2, store1
		followerID = cfg1.BrokerID
		followerCtrl = rc1
	}

	leaderAddr := binaryAddr1

	followerAddr := binaryAddr2
	if leaderID == 2 {
		leaderAddr = binaryAddr2
		followerAddr = binaryAddr1
	}

	sink := replication.NewReportingSink(replication.NewWALSink(followerStore, "alpha", 0), followerCtrl, "alpha", 0, followerID)
	pr := replication.NewPartitionReplicator(rep, api.BrokerInfo{BrokerID: leaderID, Host: leaderAddr}, "alpha", 0, sink)
	repErr := make(chan error, 1)

	go func() {
		repErr <- pr.Run(ctxSrv)
	}()

	sendProtoRequest := func(addr string, key api.APIKey, payload []byte) ([]byte, error) {
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
	sendProduce := func(addr string, records []api.Record) (*netproto.ProduceResponse, error) {
		req := &netproto.ProduceRequest{
			Topic:     "alpha",
			Partition: 0,
			Records:   records,
		}

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
	sendFetch := func(addr string, offset api.Offset) (*netproto.FetchResponse, error) {
		req := &netproto.FetchRequest{
			Topic:     "alpha",
			Partition: 0,
			Offset:    offset,
			MaxBytes:  4096,
		}

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

	if _, err := leaderBroker.Produce(ctx, "alpha", 0, []api.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
		{Value: []byte("three")},
	}); err != nil {
		t.Fatalf("produce on leader: %v", err)
	}

	// Wait for follower to catch up.
	var replicationErr error

	caughtUp := waitUntil(t, func() bool {
		select {
		case err := <-repErr:
			replicationErr = err
			return true
		default:
		}

		logFollower, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})

		return logFollower.HighWatermark() == 2
	}, 10*time.Second)
	if replicationErr != nil && !errors.Is(replicationErr, context.Canceled) {
		t.Fatalf("replication failed: %v", replicationErr)
	}

	if !caughtUp {
		logFollower, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
		t.Fatalf("follower did not catch up, hwm=%d", logFollower.HighWatermark())
	}

	// ISR should contain both.
	metaAfter, _ := rc1.GetClusterMetadata(ctx)

	var isr []int

	for _, p := range metaAfter.Partitions {
		if p.Topic == "alpha" && p.Partition == 0 {
			isr = p.ISR
		}
	}

	if len(isr) != 2 || !contains(isr, 1) || !contains(isr, 2) {
		t.Fatalf("expected both brokers in ISR, got %+v", isr)
	}

	// Produce on follower should fail with not leader.
	if _, err := followerBroker.Produce(ctx, "alpha", 0, []api.Record{{Value: []byte("nope")}}); err == nil {
		t.Fatalf("expected not leader error on follower produce")
	}

	logLeader, _ := leaderStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	logFollower, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})

	leaderHWM := logLeader.HighWatermark()
	if leaderHWM != logFollower.HighWatermark() {
		t.Fatalf("expected matching high watermarks, leader=%d follower=%d", leaderHWM, logFollower.HighWatermark())
	}

	// Binary protocol fetch on leader should see data.
	resp, err := sendFetch(leaderAddr, 0)
	if err != nil {
		t.Fatalf("leader fetch via netproto: %v", err)
	}

	if resp.Error != api.ErrNone {
		t.Fatalf("expected no error from leader fetch, got %v", resp.Error)
	}

	if len(resp.Records) != 3 {
		t.Fatalf("unexpected record count from leader fetch, got %d", len(resp.Records))
	}

	// Binary protocol produce/fetch on follower returns NotLeader.
	prodResp, err := sendProduce(followerAddr, []api.Record{{Value: []byte("netproto")}})
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

	logFollowerAfter, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	if logFollowerAfter.HighWatermark() != leaderHWM {
		t.Fatalf("expected follower hwm unchanged after netproto errors, leader=%d follower=%d", leaderHWM, logFollowerAfter.HighWatermark())
	}

	leaderMeta, err := leaderBroker.Metadata(ctx, []string{"alpha"})
	if err != nil {
		t.Fatalf("leader metadata: %v", err)
	}

	if len(leaderMeta) != 1 || leaderMeta[0].Replica.Role != api.RoleLeader {
		t.Fatalf("unexpected leader metadata: %+v", leaderMeta)
	}

	followerMeta, err := followerBroker.Metadata(ctx, []string{"alpha"})
	if err != nil {
		t.Fatalf("follower metadata: %v", err)
	}

	if len(followerMeta) != 1 || followerMeta[0].Leader != leaderID || followerMeta[0].Replica.Role != api.RoleFollower {
		t.Fatalf("unexpected follower metadata: %+v", followerMeta)
	}
}

func TestReplicationResumesAfterRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	leaderDir := t.TempDir()
	followerDir := t.TempDir()
	ctrlBase := t.TempDir()
	ctrlDir1 := filepath.Join(ctrlBase, "ctrl1")
	ctrlDir2 := filepath.Join(ctrlBase, "ctrl2")

	raftAddr1 := freeTCPAddr(t)
	raftAddr2 := freeTCPAddr(t)
	binaryAddr1 := freeTCPAddr(t)
	binaryAddr2 := freeTCPAddr(t)
	peers := []string{raftAddr1, raftAddr2}

	static := &api.StaticClusterConfig{
		ClusterID: "cluster-rf2-restart",
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: binaryAddr1},
			{BrokerID: 2, Host: binaryAddr2},
		},
	}
	initial := api.ClusterMetadata{ClusterID: static.ClusterID, Version: 1, Brokers: static.Brokers}
	baseCfg := api.BrokerConfig{
		ControllerMode:    "raft",
		RaftPeers:         peers,
		StaticCluster:     static,
		ReplicationFactor: 2,
		Replication:       true,
	}
	cfg1 := baseCfg
	cfg1.BrokerID = 1
	cfg1.RaftBindAddr = raftAddr1
	cfg1.BinaryAddr = binaryAddr1
	cfg2 := baseCfg
	cfg2.BrokerID = 2
	cfg2.RaftBindAddr = raftAddr2
	cfg2.BinaryAddr = binaryAddr2

	rc1, err := controller.NewRaftController(cfg1, initial, ctrlDir1)
	if err != nil {
		t.Fatalf("controller1: %v", err)
	}
	defer rc1.Close()

	rc2, err := controller.NewRaftController(cfg2, initial, ctrlDir2)
	if err != nil {
		t.Fatalf("controller2: %v", err)
	}
	defer rc2.Close()

	ctrls := []*controller.RaftController{rc1, rc2}

	leaderCtrl := waitForLeaderCtrl(t, ctrls)
	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: binaryAddr1}); err != nil {
		t.Fatalf("register broker1: %v", err)
	}

	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: binaryAddr2}); err != nil {
		t.Fatalf("register broker2: %v", err)
	}

	if _, err := leaderCtrl.AssignTopic(ctx, "alpha", api.TopicConfig{Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatalf("assign topic: %v", err)
	}

	waitForClusterMeta(t, ctrls, 1)

	appendMetadata := func(store *metadata.Store) {
		ev := metadata.CreateTopicEvent{
			Name:              "alpha",
			NumPartitions:     1,
			ReplicationFactor: 2,
			Partitions: []metadata.PartitionSpec{{
				ID: 0,
				Replicas: []metadata.ReplicaSpec{
					{BrokerID: 1, Role: api.RoleLeader, LeaderEpoch: 0},
					{BrokerID: 2, Role: api.RoleFollower, LeaderEpoch: 0},
				},
			}},
		}
		if err := store.AppendCreateTopic(ctx, ev); err != nil {
			t.Fatalf("append metadata: %v", err)
		}
	}

	meta1, _ := metadata.NewStore(api.BrokerConfig{DataDir: leaderDir})
	defer meta1.Close()

	appendMetadata(meta1)

	meta2, _ := metadata.NewStore(api.BrokerConfig{DataDir: followerDir})
	defer meta2.Close()

	appendMetadata(meta2)

	store1, _ := storage.NewManager(storage.Config{DataDir: leaderDir, SyncOnAppend: true})
	store2, _ := storage.NewManager(storage.Config{DataDir: followerDir, SyncOnAppend: true})

	defer store1.Close()
	defer store2.Close()

	if err := store1.Recover(ctx); err != nil {
		t.Fatalf("store1 recover: %v", err)
	}

	if err := store2.Recover(ctx); err != nil {
		t.Fatalf("store2 recover: %v", err)
	}

	offset1, _ := broker.NewOffsetStore(leaderDir)
	offset2, _ := broker.NewOffsetStore(followerDir)

	defer offset1.Close()
	defer offset2.Close()

	metaTopics1, _ := meta1.RecoverTopics(ctx)

	metaTopics2, _ := meta2.RecoverTopics(ctx)
	if len(metaTopics1.Topics) == 0 || len(metaTopics2.Topics) == 0 {
		t.Fatalf("topics not recovered")
	}

	b1, err := broker.NewBroker(cfg1, store1, offset1, meta1, rc1, nil)
	if err != nil {
		t.Fatalf("broker1: %v", err)
	}
	defer b1.Close()

	b2, err := broker.NewBroker(cfg2, store2, offset2, meta2, rc2, nil)
	if err != nil {
		t.Fatalf("broker2: %v", err)
	}
	defer b2.Close()

	meta, _ := rc1.GetClusterMetadata(ctx)

	var leaderID int

	for _, p := range meta.Partitions {
		if p.Topic == "alpha" && p.Partition == 0 {
			leaderID = p.Leader
		}
	}

	if leaderID == 0 {
		t.Fatalf("leader not found")
	}

	leaderBroker := b1
	leaderStore := store1
	followerStore := store2
	leaderAddr := binaryAddr1
	followerCtrl := controller.MetadataStore(rc2)
	followerID := cfg2.BrokerID

	if leaderID == 2 {
		leaderBroker = b2
		leaderStore, followerStore = store2, store1
		leaderAddr = binaryAddr2
		followerCtrl = rc1
		followerID = cfg1.BrokerID
	}

	server, _ := netproto.NewServer(leaderAddr, leaderBroker)

	ctxSrv, cancelSrv := context.WithCancel(ctx)
	defer cancelSrv()
	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe(ctxSrv)
	}()
	waitForServerReady(t, leaderAddr, serverErr)

	rep := replication.NewBinaryReplicator()
	startReplication := func(fStore *storage.Manager, fCtrl controller.MetadataStore) (context.CancelFunc, <-chan struct{}, <-chan error) {
		sink := replication.NewReportingSink(replication.NewWALSink(fStore, "alpha", 0), fCtrl, "alpha", 0, followerID)
		pr := replication.NewPartitionReplicator(rep, api.BrokerInfo{BrokerID: leaderID, Host: leaderAddr}, "alpha", 0, sink)
		ctxRep, cancelRep := context.WithCancel(ctx)
		done := make(chan struct{})
		errCh := make(chan error, 1)

		go func() {
			errCh <- pr.Run(ctxRep)

			close(done)
		}()

		return cancelRep, done, errCh
	}

	waitCatchUp := func(expected api.Offset, errCh <-chan error) {
		ok := waitUntil(t, func() bool {
			select {
			case err := <-errCh:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("replication error: %v", err)
				}

				return false
			default:
			}

			log, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})

			return log.HighWatermark() == expected
		}, 5*time.Second)
		if !ok {
			log, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
			t.Fatalf("follower did not reach hwm %d, got %d", expected, log.HighWatermark())
		}
	}

	produce := func(values ...string) {
		recs := make([]api.Record, 0, len(values))
		for _, v := range values {
			recs = append(recs, api.Record{Value: []byte(v)})
		}

		if _, err := leaderBroker.Produce(ctx, "alpha", 0, recs); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	cancelRep, repDone, repErr := startReplication(followerStore, followerCtrl)

	produce("one", "two")
	waitCatchUp(1, repErr)

	if log, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0}); log.HighWatermark() != 1 {
		t.Fatalf("expected follower hwm 1 after initial replication, got %d", log.HighWatermark())
	}

	cancelRep()
	<-repDone

	produce("three", "four")

	cancelRep, repDone, repErr = startReplication(followerStore, followerCtrl)
	waitCatchUp(3, repErr)

	if log, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0}); log.HighWatermark() != 3 {
		t.Fatalf("expected follower hwm 3 after restart catch-up, got %d", log.HighWatermark())
	}

	if recs, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0}); len(offsetsOf(mustRead(ctx, recs))) != 4 {
		t.Fatalf(
			"unexpected follower offsets after catch-up: start=%d hwm=%d offsets=%v",
			recs.StartOffset(),
			recs.HighWatermark(),
			offsetsOf(mustRead(ctx, recs)),
		)
	}

	cancelRep()
	<-repDone

	rc1.Close()
	rc2.Close()

	rc1, _ = controller.NewRaftController(cfg1, initial, ctrlDir1)
	rc2, _ = controller.NewRaftController(cfg2, initial, ctrlDir2)

	defer rc1.Close()
	defer rc2.Close()

	ctrls = []*controller.RaftController{rc1, rc2}

	leaderCtrl = waitForLeaderCtrl(t, ctrls)
	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 1, Host: binaryAddr1}); err != nil {
		t.Fatalf("re-register broker1: %v", err)
	}

	if err := leaderCtrl.RegisterBroker(ctx, api.BrokerInfo{BrokerID: 2, Host: binaryAddr2}); err != nil {
		t.Fatalf("re-register broker2: %v", err)
	}

	followerCtrl = controller.MetadataStore(rc2)
	if followerID == cfg1.BrokerID {
		followerCtrl = rc1
	}

	cancelRep, repDone, repErr = startReplication(followerStore, followerCtrl)

	produce("five", "six")
	waitCatchUp(5, repErr)

	if log, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0}); log.HighWatermark() != 5 {
		t.Fatalf("expected follower hwm 5 after controller restart, got %d", log.HighWatermark())
	}

	if recs, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0}); len(offsetsOf(mustRead(ctx, recs))) != 6 {
		t.Fatalf(
			"unexpected follower offsets after final catch-up: start=%d hwm=%d offsets=%v",
			recs.StartOffset(),
			recs.HighWatermark(),
			offsetsOf(mustRead(ctx, recs)),
		)
	}

	cancelRep()
	<-repDone

	leaderLog, _ := leaderStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	followerLog, _ := followerStore.OpenLog(storage.LogOptions{Topic: "alpha", Partition: 0})
	leadRecords, _ := leaderLog.Read(ctx, 0, 1<<20)

	follRecords, _ := followerLog.Read(ctx, 0, 1<<20)
	if followerLog.StartOffset() != 0 {
		t.Fatalf("unexpected follower start offset %d", followerLog.StartOffset())
	}

	if len(leadRecords) != len(follRecords) {
		followerOffsets := make([]api.Offset, 0, len(follRecords))
		for _, r := range follRecords {
			followerOffsets = append(followerOffsets, r.Offset)
		}

		t.Fatalf(
			"record count mismatch leader=%d follower=%d hwm=%d offsets=%v",
			len(leadRecords),
			len(follRecords),
			followerLog.HighWatermark(),
			followerOffsets,
		)
	}

	for i := range leadRecords {
		if !bytes.Equal(leadRecords[i].Value, follRecords[i].Value) || leadRecords[i].Offset != follRecords[i].Offset {
			t.Fatalf("mismatched record at %d: leader=%s follower=%s", i, leadRecords[i].Value, follRecords[i].Value)
		}
	}
}

func offsetsOf(recs []api.Record) []api.Offset {
	res := make([]api.Offset, 0, len(recs))
	for _, r := range recs {
		res = append(res, r.Offset)
	}

	return res
}

func mustRead(ctx context.Context, log storage.Log) []api.Record {
	recs, err := log.Read(ctx, log.StartOffset(), 1<<20)
	if err != nil {
		panic(err)
	}

	return recs
}

func freeTCPAddr(t *testing.T) string {
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

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range ctrls {
			if c.RaftState() == raft.Leader.String() {
				return c
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("leader not elected")

	return nil
}

func waitForClusterMeta(t *testing.T, ctrls []*controller.RaftController, parts int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
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

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("metadata did not converge")
}

func contains(list []int, id int) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}

	return false
}

func waitUntil(t *testing.T, pred func() bool, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}

		time.Sleep(20 * time.Millisecond)
	}

	return false
}

func waitForServerReady(t *testing.T, addr string, errCh <-chan error) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("server %s failed before ready: %v", addr, err)
			}

			t.Fatalf("server %s stopped before ready", addr)
		default:
		}

		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("server %s did not become ready in time", addr)
}
