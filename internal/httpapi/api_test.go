package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func setupTestServer(t *testing.T) (*httptest.Server, *broker.Broker, *storage.Manager, *broker.OffsetStore) {
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
	b, err := broker.NewBroker(api.BrokerConfig{
		BrokerID:          1,
		DataDir:           dir,
		ReplicationFactor: 1,
	}, store, offsetStore, metaStore)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	handler := New(b, api.BrokerConfig{BrokerID: 1, BinaryAddr: ":7912", MQTTAddr: ":1883", HTTPAddr: ":8090"})
	mux := http.NewServeMux()
	handler.Register(mux)
	return httptest.NewServer(mux), b, store, offsetStore
}

func TestCreateTopicEndpoint(t *testing.T) {
	server, b, store, offsetStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()

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
}

func TestProduceEndpoint(t *testing.T) {
	server, b, store, offsetStore := setupTestServer(t)
	defer server.Close()
	defer b.Close()
	defer store.Close()
	defer offsetStore.Close()
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
