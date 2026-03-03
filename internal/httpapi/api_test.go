package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func setupTestServer(t *testing.T) (*httptest.Server, *broker.Broker, *storage.Manager, *broker.OffsetStore, *metadata.Store) {
	return setupTestServerWithRF(t, 1)
}

func setupTestServerWithRF(
	t *testing.T,
	rf int,
) (*httptest.Server, *broker.Broker, *storage.Manager, *broker.OffsetStore, *metadata.Store) {
	t.Helper()
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	recovered, err := metaStore.RecoverTopics(context.Background())
	if err != nil {
		t.Fatalf("recover topics: %v", err)
	}

	cfg := api.BrokerConfig{
		BrokerID:          1,
		BinaryAddr:        ":7912",
		MQTTAddr:          ":1883",
		HTTPAddr:          ":8090",
		ReplicationFactor: rf,
		ControllerMode:    "single",
		ClusterID:         "test-cluster",
	}

	ctrl, err := controller.NewSingleNodeController(cfg, recovered.Topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		ReplicationFactor: rf,
	}, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)

	return httptest.NewServer(mux), b, store, offsetStore, metaStore
}

type followerCtrl struct {
	meta api.ClusterMetadata
}

func (f *followerCtrl) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	return f.meta, nil
}

func (f *followerCtrl) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata, 1)
	ch <- f.meta

	close(ch)

	return ch, nil
}

func (f *followerCtrl) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func (f *followerCtrl) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return f.meta, nil
}

func (f *followerCtrl) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return f.meta, nil
}

type assignmentController struct {
	meta           api.ClusterMetadata
	topicPlans     map[string][]api.PartitionAssignment
	assignRequests int
}

type forwardingFollowerCtrl struct {
	meta        api.ClusterMetadata
	leader      string
	assignCalls int
}

func (f *forwardingFollowerCtrl) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	return f.meta, nil
}

func (f *forwardingFollowerCtrl) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata, 1)
	ch <- f.meta

	close(ch)

	return ch, nil
}

func (f *forwardingFollowerCtrl) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func (f *forwardingFollowerCtrl) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	f.assignCalls++

	return f.meta, controller.NotLeaderError{Leader: f.leader}
}

func (f *forwardingFollowerCtrl) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return f.meta, nil
}

func (f *forwardingFollowerCtrl) RaftState() string { return "follower" }

func (f *forwardingFollowerCtrl) RaftLeader() string { return f.leader }

type leaderUnavailableCtrl struct {
	meta api.ClusterMetadata
}

func (l *leaderUnavailableCtrl) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	return l.meta, nil
}

func (l *leaderUnavailableCtrl) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata, 1)
	ch <- l.meta

	close(ch)

	return ch, nil
}

func (l *leaderUnavailableCtrl) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func (l *leaderUnavailableCtrl) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return l.meta, controller.ErrLeaderNotElected
}

func (l *leaderUnavailableCtrl) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return l.meta, nil
}

func (a *assignmentController) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	return a.meta, nil
}

func (a *assignmentController) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata, 1)
	ch <- a.meta

	close(ch)

	return ch, nil
}

func (a *assignmentController) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx

	a.meta.Brokers = append(a.meta.Brokers, info)

	return nil
}

func (a *assignmentController) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = cfg

	if plans, ok := a.topicPlans[name]; ok {
		a.meta.Partitions = append(a.meta.Partitions, plans...)
		a.assignRequests++
		a.meta.Version++
	}

	return a.meta, nil
}

func (a *assignmentController) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return a.meta, nil
}

func TestHTTPProduceNotLeader(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	ev := metadata.CreateTopicEvent{
		Name:              "alpha",
		NumPartitions:     1,
		ReplicationFactor: 2,
		Partitions: []metadata.PartitionSpec{
			{ID: 0, Replicas: []metadata.ReplicaSpec{
				{BrokerID: 1, Role: api.RoleFollower, LeaderEpoch: 0},
				{BrokerID: 2, Role: api.RoleLeader, LeaderEpoch: 0},
			}},
		},
	}
	if err := metaStore.AppendCreateTopic(context.Background(), ev); err != nil {
		t.Fatalf("append create topic: %v", err)
	}

	meta := api.ClusterMetadata{
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
		Partitions: []api.PartitionAssignment{
			{Topic: "alpha", Partition: 0, Replicas: []int{1, 2}, Leader: 2, ISR: []int{2}},
		},
	}
	ctrl := &followerCtrl{meta: meta}
	cfg := api.BrokerConfig{
		BrokerID:          1,
		BinaryAddr:        ":7912",
		MQTTAddr:          ":1883",
		HTTPAddr:          ":8090",
		ReplicationFactor: 2,
		ControllerMode:    "raft",
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)

	defer func() {
		server.Close()
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()

	body := []byte(`{"value":"hello"}`)

	resp, err := http.Post(server.URL+"/api/topics/alpha/partitions/0/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out["error"] != "not_leader" || int(out["leaderBrokerID"].(float64)) != 2 {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestHTTPFetchNotLeader(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	ev := metadata.CreateTopicEvent{
		Name:              "alpha",
		NumPartitions:     1,
		ReplicationFactor: 2,
		Partitions: []metadata.PartitionSpec{
			{ID: 0, Replicas: []metadata.ReplicaSpec{
				{BrokerID: 1, Role: api.RoleFollower, LeaderEpoch: 0},
				{BrokerID: 2, Role: api.RoleLeader, LeaderEpoch: 0},
			}},
		},
	}
	if err := metaStore.AppendCreateTopic(context.Background(), ev); err != nil {
		t.Fatalf("append create topic: %v", err)
	}

	meta := api.ClusterMetadata{
		Brokers: []api.BrokerInfo{
			{BrokerID: 1, Host: "b1"},
			{BrokerID: 2, Host: "b2"},
		},
		Partitions: []api.PartitionAssignment{
			{Topic: "alpha", Partition: 0, Replicas: []int{1, 2}, Leader: 2, ISR: []int{2}},
		},
	}
	ctrl := &followerCtrl{meta: meta}
	cfg := api.BrokerConfig{
		BrokerID:          1,
		BinaryAddr:        ":7912",
		MQTTAddr:          ":1883",
		HTTPAddr:          ":8090",
		ReplicationFactor: 2,
		ControllerMode:    "raft",
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)

	defer func() {
		server.Close()
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()

	resp, err := http.Get(server.URL + "/api/topics/alpha/partitions/0/messages")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out["error"] != "not_leader" || int(out["leaderBrokerID"].(float64)) != 2 {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestCreateTopicEndpoint(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	body := []byte(`{"name":"api-topic","partitions":1,"replicationFactor":1}`)

	resp, err := http.Post(server.URL+"/api/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var detail broker.TopicDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if detail.Name != "api-topic" || detail.PartitionCount != 1 {
		t.Fatalf("unexpected detail: %+v", detail)
	}

	if len(detail.Partitions) != 1 {
		t.Fatalf("expected 1 partition detail, got %d", len(detail.Partitions))
	}

	part := detail.Partitions[0]
	if part.Role != "leader" {
		t.Fatalf("expected leader role, got %+v", part)
	}

	if len(part.ISR) != 1 || part.ISR[0] != 1 {
		t.Fatalf("unexpected ISR: %+v", part)
	}

	if part.LeaderEpoch < 0 {
		t.Fatalf("expected non-negative leader epoch, got %+v", part)
	}

	if len(part.Replicas) != 1 || part.Replicas[0] != 1 {
		t.Fatalf("unexpected replicas: %+v", part.Replicas)
	}
	// verify controller mode is exposed
	brokerResp, err := http.Get(server.URL + "/api/broker")
	if err != nil {
		t.Fatalf("get broker: %v", err)
	}

	var brokerInfo map[string]interface{}
	if err := json.NewDecoder(brokerResp.Body).Decode(&brokerInfo); err != nil {
		t.Fatalf("decode broker: %v", err)
	}

	_ = brokerResp.Body.Close()

	if brokerInfo["controllerMode"] != "single" {
		t.Fatalf("expected controllerMode single, got %v", brokerInfo["controllerMode"])
	}
}

func TestCreateTopicEndpointReplicationFactor(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	body := []byte(`{"name":"rf-topic","partitions":1,"replicationFactor":3}`)

	resp, err := http.Post(server.URL+"/api/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	recovered, err := metaStore.RecoverTopics(context.Background())
	if err != nil {
		t.Fatalf("recover topics: %v", err)
	}

	topic, ok := recovered.Topics["rf-topic"]
	if !ok {
		t.Fatalf("rf-topic not found in metadata")
	}

	if topic.ReplicationFactor != 1 {
		t.Fatalf("expected rf 1 from controller assignments, got %d", topic.ReplicationFactor)
	}

	if len(topic.Partitions) != 1 || len(topic.Partitions[0].Replicas) != 1 {
		t.Fatalf("expected 1 partition with 1 replica, got %+v", topic.Partitions)
	}
}

func TestCreateTopicEndpointDefaultsReplicationFactor(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServerWithRF(t, 2)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	body := []byte(`{"name":"rf-default","partitions":1,"replicationFactor":0}`)

	resp, err := http.Post(server.URL+"/api/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	recovered, err := metaStore.RecoverTopics(context.Background())
	if err != nil {
		t.Fatalf("recover topics: %v", err)
	}

	topic, ok := recovered.Topics["rf-default"]
	if !ok {
		t.Fatalf("rf-default not found in metadata")
	}

	if topic.ReplicationFactor != 1 {
		t.Fatalf("expected rf 1 from controller assignments, got %d", topic.ReplicationFactor)
	}

	if len(topic.Partitions) != 1 || len(topic.Partitions[0].Replicas) != 1 {
		t.Fatalf("expected replicas from controller assignments, got %+v", topic.Partitions)
	}

	r := topic.Partitions[0].Replicas[0]
	if r.BrokerID != 1 {
		t.Fatalf("expected broker 1 in replicas, got %+v", topic.Partitions[0].Replicas)
	}
}

func TestCreateTopicEndpointForwardsToRaftLeader(t *testing.T) {
	leaderCalls := 0

	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaderCalls++

		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}

		if r.URL.Path != "/api/topics" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		if got := r.Header.Get(forwardedCreateTopicHeader); got != "1" {
			t.Fatalf("expected forwarding header, got %q", got)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"forwarded-topic","partitionCount":1}`))
	}))
	defer leaderSrv.Close()

	leaderURL, err := url.Parse(leaderSrv.URL)
	if err != nil {
		t.Fatalf("parse leader url: %v", err)
	}

	leaderHost, _, err := net.SplitHostPort(leaderURL.Host)
	if err != nil {
		t.Fatalf("split leader host: %v", err)
	}

	leaderRaftAddr := net.JoinHostPort(leaderHost, "9001")

	ctrl := &forwardingFollowerCtrl{
		meta: api.ClusterMetadata{
			ClusterID: "cluster-forward",
			Version:   1,
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1"},
				{BrokerID: 2, Host: "b2", HTTPAddr: leaderURL.Host, ControllerAddr: leaderRaftAddr},
			},
		},
		leader: leaderRaftAddr,
	}

	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer store.Close()

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}
	defer offsetStore.Close()

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}
	defer metaStore.Close()

	cfg := api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		BinaryAddr:        ":7912",
		MQTTAddr:          ":1883",
		HTTPAddr:          ":6555",
		ReplicationFactor: 2,
		ControllerMode:    "raft",
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"name":"api-topic","partitions":1,"replicationFactor":2}`)

	resp, err := http.Post(server.URL+"/api/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if payload["name"] != "forwarded-topic" {
		t.Fatalf("unexpected forwarded payload: %+v", payload)
	}

	if leaderCalls != 1 {
		t.Fatalf("expected one leader call, got %d", leaderCalls)
	}

	if ctrl.assignCalls != 1 {
		t.Fatalf("local assign should be called exactly once before forward, got %d", ctrl.assignCalls)
	}
}

func TestCreateTopicEndpointReturnsNotLeaderContractWithoutLeaderHTTPAddr(t *testing.T) {
	leaderAddr := "leader-node:9001"

	ctrl := &forwardingFollowerCtrl{
		meta: api.ClusterMetadata{
			ClusterID: "cluster-forward",
			Version:   1,
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, Host: "b1"},
				{BrokerID: 2, Host: "leader-node:7912", ControllerAddr: leaderAddr},
			},
		},
		leader: leaderAddr,
	}

	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer store.Close()

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}
	defer offsetStore.Close()

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}
	defer metaStore.Close()

	cfg := api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		BinaryAddr:        ":7912",
		MQTTAddr:          ":1883",
		HTTPAddr:          ":8090",
		ReplicationFactor: 2,
		ControllerMode:    "raft",
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"name":"api-topic","partitions":1,"replicationFactor":2}`)

	resp, err := http.Post(server.URL+"/api/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if payload["error"] != "not_leader" {
		t.Fatalf("expected not_leader error payload, got %+v", payload)
	}

	if payload["leader"] != leaderAddr {
		t.Fatalf("expected leader hint %q, got %+v", leaderAddr, payload)
	}
}

func TestCreateTopicEndpointReturnsLeaderNotElected(t *testing.T) {
	ctrl := &leaderUnavailableCtrl{
		meta: api.ClusterMetadata{
			ClusterID: "cluster-unavailable",
			Version:   1,
			Brokers:   []api.BrokerInfo{{BrokerID: 1, Host: "b1"}},
		},
	}

	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer store.Close()

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}
	defer offsetStore.Close()

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}
	defer metaStore.Close()

	cfg := api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		BinaryAddr:        ":7912",
		MQTTAddr:          ":1883",
		HTTPAddr:          ":8090",
		ReplicationFactor: 2,
		ControllerMode:    "raft",
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"name":"api-topic","partitions":1,"replicationFactor":2}`)

	resp, err := http.Post(server.URL+"/api/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if payload["error"] != "leader_not_elected" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestControllerStatusEndpoint(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	resp, err := http.Get(server.URL + "/api/controller")
	if err != nil {
		t.Fatalf("get controller: %v", err)
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if status["mode"] != "single" {
		t.Fatalf("expected mode single, got %v", status["mode"])
	}

	if status["raftState"] != "none" {
		t.Fatalf("expected raftState none, got %v", status["raftState"])
	}

	if status["term"].(float64) != 0 {
		t.Fatalf("expected term 0, got %v", status["term"])
	}

	if peers, ok := status["peers"].([]interface{}); !ok || len(peers) != 0 {
		t.Fatalf("expected empty peers, got %v", status["peers"])
	}

	if status["clusterID"] == "" {
		t.Fatalf("expected clusterID")
	}

	if status["version"] == nil {
		t.Fatalf("expected version")
	}
}

func TestControllerStatusEndpointWithoutController(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		ReplicationFactor: 1,
	}, store, offsetStore, metaStore, nil, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	cfg := api.BrokerConfig{
		BrokerID:       1,
		ControllerMode: "single",
	}

	handler := New(b, cfg, nil)
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)

	defer func() {
		server.Close()
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()

	resp, err := http.Get(server.URL + "/api/controller")
	if err != nil {
		t.Fatalf("get controller: %v", err)
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if status["mode"] != "single" {
		t.Fatalf("expected mode single, got %v", status["mode"])
	}

	if status["raftState"] != "none" {
		t.Fatalf("expected raftState none, got %v", status["raftState"])
	}

	if status["clusterID"] != "" {
		t.Fatalf("expected empty clusterID, got %v", status["clusterID"])
	}
}

func TestControllerStatusEndpointRaft(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	initial := api.ClusterMetadata{ClusterID: "cluster-raft", Version: 1, Brokers: []api.BrokerInfo{{BrokerID: 1}}}
	cfg := api.BrokerConfig{
		BrokerID:          1,
		BinaryAddr:        ":7912",
		MQTTAddr:          ":1883",
		HTTPAddr:          ":8090",
		ReplicationFactor: 1,
		ControllerMode:    "raft",
	}

	ctrl, err := controller.NewRaftController(cfg, initial, "")
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	defer ctrl.Close()

	if err := ctrl.RegisterBroker(context.Background(), api.BrokerInfo{BrokerID: 1, Host: "b1"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		ReplicationFactor: 1,
	}, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)

	defer func() {
		server.Close()
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()

	resp, err := http.Get(server.URL + "/api/controller")
	if err != nil {
		t.Fatalf("get controller: %v", err)
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if status["mode"] != "raft" {
		t.Fatalf("expected mode raft, got %v", status["mode"])
	}

	state, _ := status["raftState"].(string)
	if state == "" {
		t.Fatalf("expected raftState set")
	}

	termVal, _ := status["term"].(float64)
	if termVal < 1 {
		t.Fatalf("expected term >=1, got %v", termVal)
	}

	if peers, ok := status["peers"].([]interface{}); !ok || len(peers) == 0 {
		t.Fatalf("expected non-empty peers, got %v", status["peers"])
	}
}

func TestProduceEndpoint(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "p", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	body := []byte(`{"value":"hello"}`)

	resp, err := http.Post(server.URL+"/api/topics/p/partitions/0/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out["partition"].(float64) != 0 {
		t.Fatalf("unexpected partition: %v", out)
	}
}

func TestTopicProduceByKeyEndpoint(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "keyed", api.TopicConfig{Partitions: 3}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	send := func(value string) map[string]interface{} {
		body := []byte(`{"key":"sensor-7","value":"` + value + `"}`)

		resp, err := http.Post(server.URL+"/api/topics/keyed/messages", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}

		var out map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}

		return out
	}

	first := send("v1")
	second := send("v2")

	if first["partition"] != second["partition"] {
		t.Fatalf("same key routed to different partitions: %v vs %v", first, second)
	}
}

func TestTopicMessagesEndpointAggregatesAllPartitions(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "agg", api.TopicConfig{Partitions: 2}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	if _, err := b.Produce(ctx, "agg", 0, []api.Record{{Value: []byte("p0")}}); err != nil {
		t.Fatalf("produce p0: %v", err)
	}

	if _, err := b.Produce(ctx, "agg", 1, []api.Record{{Value: []byte("p1")}}); err != nil {
		t.Fatalf("produce p1: %v", err)
	}

	resp, err := http.Get(server.URL + "/api/topics/agg/messages?limit=10")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(out) < 2 {
		t.Fatalf("expected at least two messages, got %d", len(out))
	}

	hasP0 := false
	hasP1 := false

	for _, m := range out {
		switch int(m["partition"].(float64)) {
		case 0:
			hasP0 = true
		case 1:
			hasP1 = true
		}
	}

	if !hasP0 || !hasP1 {
		t.Fatalf("expected messages from partitions 0 and 1, got %+v", out)
	}
}

func TestClusterMetadataEndpoint(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	offsetStore, err := broker.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}

	metaStore, err := metadata.NewStore(api.BrokerConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	cfg := api.BrokerConfig{BrokerID: 1, BinaryAddr: ":7912", MQTTAddr: ":1883", HTTPAddr: ":8090", ReplicationFactor: 1}

	ctrl, err := controller.NewSingleNodeController(cfg, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	b, err := broker.NewBroker(cfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)

	defer func() {
		server.Close()
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()
	// create topics via HTTP to trigger controller.AssignTopic
	for _, topic := range []struct {
		name       string
		partitions int
	}{
		{"alpha", 2},
		{"beta", 1},
	} {
		body := map[string]interface{}{
			"name":              topic.name,
			"partitions":        topic.partitions,
			"replicationFactor": 1,
		}
		data, _ := json.Marshal(body)

		resp, err := http.Post(server.URL+"/api/topics", "application/json", bytes.NewReader(data))
		if err != nil {
			t.Fatalf("post topic %s: %v", topic.name, err)
		}

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
			t.Fatalf("unexpected status %d for topic %s", resp.StatusCode, topic.name)
		}

		resp.Body.Close()
	}

	resp, err := http.Get(server.URL + "/api/cluster")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	defer resp.Body.Close()

	var meta api.ClusterMetadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(meta.Brokers) != 1 || meta.Brokers[0].BrokerID != cfg.BrokerID {
		t.Fatalf("unexpected brokers: %+v", meta.Brokers)
	}

	if len(meta.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(meta.Partitions))
	}

	for _, p := range meta.Partitions {
		if p.Leader != cfg.BrokerID {
			t.Fatalf("unexpected leader %d for %s-%d", p.Leader, p.Topic, p.Partition)
		}

		if len(p.Replicas) != 1 || p.Replicas[0] != cfg.BrokerID {
			t.Fatalf("unexpected replicas for %s-%d: %+v", p.Topic, p.Partition, p.Replicas)
		}
	}
}

func TestClusterMetadataEndpointJSONKeys(t *testing.T) {
	server, b, store, offsetStore, metaStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
	defer metaStore.Close()

	resp, err := http.Get(server.URL + "/api/cluster")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	defer resp.Body.Close()

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, key := range []string{"clusterID", "version", "brokers", "partitions"} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("missing lowercase key %q in cluster response: %+v", key, raw)
		}
	}

	for _, key := range []string{"ClusterID", "Version", "Brokers", "Partitions"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("unexpected legacy key %q in cluster response", key)
		}
	}
}

func TestCreateTopicRespectsControllerAssignmentsAcrossBrokers(t *testing.T) {
	assignments := []api.PartitionAssignment{
		{Topic: "alpha", Partition: 0, Replicas: []int{1}, ISR: []int{1}, Leader: 1},
		{Topic: "alpha", Partition: 1, Replicas: []int{2}, ISR: []int{2}, Leader: 2},
	}
	ctrl := &assignmentController{
		meta: api.ClusterMetadata{
			ClusterID: "cluster-assign",
			Version:   1,
			Brokers:   []api.BrokerInfo{{BrokerID: 1}, {BrokerID: 2}},
		},
		topicPlans: map[string][]api.PartitionAssignment{"alpha": assignments},
	}

	dir1 := t.TempDir()

	store1, err := storage.NewManager(storage.Config{DataDir: dir1})
	if err != nil {
		t.Fatalf("store1: %v", err)
	}

	t.Cleanup(func() { _ = store1.Close() })

	offset1, err := broker.NewOffsetStore(dir1)
	if err != nil {
		t.Fatalf("offset1: %v", err)
	}

	t.Cleanup(func() { _ = offset1.Close() })

	meta1, err := metadata.NewStore(api.BrokerConfig{DataDir: dir1})
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}

	t.Cleanup(func() { _ = meta1.Close() })

	cfg1 := api.BrokerConfig{BrokerID: 1, DataDir: dir1, ReplicationFactor: 1, ControllerMode: "raft"}

	b1, err := broker.NewBroker(cfg1, store1, offset1, meta1, ctrl, nil)
	if err != nil {
		t.Fatalf("broker1: %v", err)
	}

	t.Cleanup(func() { _ = b1.Close() })

	handler := New(b1, cfg1, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := map[string]interface{}{
		"name":              "alpha",
		"partitions":        2,
		"replicationFactor": 1,
	}
	data, _ := json.Marshal(body)

	resp, err := http.Post(srv.URL+"/api/topics", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	if ctrl.assignRequests != 1 {
		t.Fatalf("expected controller assign once, got %d", ctrl.assignRequests)
	}

	parts1 := b1.TopicsSnapshot()
	if len(parts1) != 1 || parts1[0].Name != "alpha" || parts1[0].Partitions != 1 {
		t.Fatalf("broker1 should have one local partition, got %+v", parts1)
	}

	if _, ok := b1.TopicDetail("alpha"); !ok {
		t.Fatalf("broker1 missing topic detail")
	}

	dir2 := t.TempDir()

	store2, err := storage.NewManager(storage.Config{DataDir: dir2})
	if err != nil {
		t.Fatalf("store2: %v", err)
	}

	t.Cleanup(func() { _ = store2.Close() })

	offset2, err := broker.NewOffsetStore(dir2)
	if err != nil {
		t.Fatalf("offset2: %v", err)
	}

	t.Cleanup(func() { _ = offset2.Close() })

	meta2, err := metadata.NewStore(api.BrokerConfig{DataDir: dir2})
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}

	t.Cleanup(func() { _ = meta2.Close() })

	cfg2 := api.BrokerConfig{BrokerID: 2, DataDir: dir2, ReplicationFactor: 1, ControllerMode: "raft"}

	b2, err := broker.NewBroker(cfg2, store2, offset2, meta2, ctrl, nil)
	if err != nil {
		t.Fatalf("broker2: %v", err)
	}

	t.Cleanup(func() { _ = b2.Close() })

	parts2 := b2.TopicsSnapshot()
	if len(parts2) != 1 || parts2[0].Partitions != 1 {
		t.Fatalf("broker2 should have one local partition, got %+v", parts2)
	}

	if detail, ok := b2.TopicDetail("alpha"); !ok || detail.PartitionCount != 1 {
		t.Fatalf("broker2 expected one local partition, got %+v", detail)
	}
}

type recordingDoer struct {
	requests []*http.Request
	status   int
	body     string
}

func (d *recordingDoer) Do(req *http.Request) (*http.Response, error) {
	d.requests = append(d.requests, req.Clone(req.Context()))

	return &http.Response{
		StatusCode: d.status,
		Body:       io.NopCloser(bytes.NewBufferString(d.body)),
		Header:     make(http.Header),
	}, nil
}

func TestForwardToURLUsesInjectedHTTPClient(t *testing.T) {
	doer := &recordingDoer{
		status: http.StatusCreated,
		body:   `{"ok":true}`,
	}
	h := NewWithHTTPClient(nil, api.BrokerConfig{}, nil, doer)
	req := httptest.NewRequest(http.MethodPost, "http://follower/api/topics", http.NoBody)

	status, payload, ok := h.forwardToURL(req, http.MethodPost, "http://leader/api/topics", []byte(`{"name":"t"}`))
	if !ok {
		t.Fatalf("expected forwarding success")
	}

	if status != http.StatusCreated {
		t.Fatalf("status=%d want=%d", status, http.StatusCreated)
	}

	if string(payload) != `{"ok":true}` {
		t.Fatalf("unexpected payload: %s", string(payload))
	}

	if len(doer.requests) != 1 {
		t.Fatalf("expected one forwarded request, got %d", len(doer.requests))
	}

	got := doer.requests[0]
	if got.Header.Get(forwardedCreateTopicHeader) != "1" {
		t.Fatalf("missing forwarding header")
	}

	if got.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("content type not set, got %q", got.Header.Get("Content-Type"))
	}
}
