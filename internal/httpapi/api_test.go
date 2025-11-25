package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func setupTestServer(t *testing.T) (*httptest.Server, *broker.Broker, *storage.Manager, *broker.OffsetStore, *metadata.Store) {
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
	cfg := api.BrokerConfig{BrokerID: 1, BinaryAddr: ":7912", MQTTAddr: ":1883", HTTPAddr: ":8090", ReplicationFactor: 1, ControllerMode: "single"}
	ctrl, err := controller.NewSingleNodeController(cfg, recovered.Topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		ReplicationFactor: 1,
	}, store, offsetStore, metaStore, ctrl)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	handler := New(b, cfg, ctrl)
	mux := http.NewServeMux()
	handler.Register(mux)
	return httptest.NewServer(mux), b, store, offsetStore, metaStore
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
	b, err := broker.NewBroker(cfg, store, offsetStore, metaStore, ctrl)
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
