package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	brokerpkg "github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/httpapi"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestBrokerRecoversTopicsFromMetadataLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	brokerCfg := api.BrokerConfig{
		BrokerID:          1,
		ReplicationFactor: 1,
		DataDir:           dir,
	}
	store, err := storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	if err := store.Recover(ctx); err != nil {
		t.Fatalf("storage recover: %v", err)
	}
	offsetStore, err := brokerpkg.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store: %v", err)
	}
	metaStore, err := metadata.NewStore(brokerCfg)
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}
	b, err := brokerpkg.NewBroker(brokerCfg, store, offsetStore, metaStore)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	server := newHTTPTestServer(b, brokerCfg)
	client := server.Client()

	createTopicHTTP(t, client, server.URL, "alpha", 2)
	createTopicHTTP(t, client, server.URL, "beta", 1)
	produceHTTP(t, client, server.URL, "alpha", 0, "alpha-0")
	produceHTTP(t, client, server.URL, "alpha", 1, "alpha-1")
	produceHTTP(t, client, server.URL, "beta", 0, "beta-0")

	server.Close()
	_ = b.Close()
	_ = store.Close()
	_ = offsetStore.Close()
	_ = metaStore.Close()

	// Restart using same data dir.
	store, err = storage.NewManager(storage.Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("storage reopen: %v", err)
	}
	if err := store.Recover(ctx); err != nil {
		t.Fatalf("storage recover after restart: %v", err)
	}
	offsetStore, err = brokerpkg.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("offset store reopen: %v", err)
	}
	metaStore, err = metadata.NewStore(brokerCfg)
	if err != nil {
		t.Fatalf("metadata store reopen: %v", err)
	}
	b, err = brokerpkg.NewBroker(brokerCfg, store, offsetStore, metaStore)
	if err != nil {
		t.Fatalf("broker reopen: %v", err)
	}
	server = newHTTPTestServer(b, brokerCfg)
	defer func() {
		server.Close()
		b.Close()
		store.Close()
		offsetStore.Close()
		metaStore.Close()
	}()

	topics := fetchTopics(t, server.URL)
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics after restart, got %d", len(topics))
	}
	if topics["alpha"].Partitions != 2 || topics["beta"].Partitions != 1 {
		t.Fatalf("unexpected topic layout after restart: %+v", topics)
	}
	alphaDetail := fetchTopicDetail(t, server.URL, "alpha")
	if alphaDetail.PartitionCount != 2 {
		t.Fatalf("expected alpha to have 2 partitions, got %d", alphaDetail.PartitionCount)
	}
	// Stored messages should survive restart.
	msgs := fetchMessages(t, server.URL, "alpha", 0)
	found := false
	for _, m := range msgs {
		if val, ok := m["value"].(string); ok && val == "alpha-0" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("did not find alpha-0 message after restart, got %v", msgs)
	}
}

func newHTTPTestServer(b *brokerpkg.Broker, cfg api.BrokerConfig) *httptest.Server {
	handler := httpapi.New(b, cfg)
	mux := http.NewServeMux()
	handler.Register(mux)
	return httptest.NewServer(mux)
}

func createTopicHTTP(t *testing.T, client *http.Client, baseURL, name string, partitions int) {
	t.Helper()
	body := map[string]interface{}{
		"name":              name,
		"partitions":        partitions,
		"replicationFactor": 1,
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal topic: %v", err)
	}
	resp, err := client.Post(baseURL+"/api/topics", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("post topic: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Fatalf("unexpected status %d creating topic %s", resp.StatusCode, name)
	}
}

func produceHTTP(t *testing.T, client *http.Client, baseURL, topic string, partition int, value string) {
	t.Helper()
	body := map[string]interface{}{"value": value}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal produce: %v", err)
	}
	url := fmt.Sprintf("%s/api/topics/%s/partitions/%d/messages", baseURL, topic, partition)
	resp, err := client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("post produce: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d producing to %s/%d", resp.StatusCode, topic, partition)
	}
}

func fetchTopics(t *testing.T, baseURL string) map[string]brokerpkg.TopicSummary {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/topics")
	if err != nil {
		t.Fatalf("get topics: %v", err)
	}
	defer resp.Body.Close()
	var summaries []brokerpkg.TopicSummary
	if err := json.NewDecoder(resp.Body).Decode(&summaries); err != nil {
		t.Fatalf("decode topics: %v", err)
	}
	res := make(map[string]brokerpkg.TopicSummary)
	for _, s := range summaries {
		res[s.Name] = s
	}
	return res
}

func fetchTopicDetail(t *testing.T, baseURL, name string) brokerpkg.TopicDetail {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/topics/" + name)
	if err != nil {
		t.Fatalf("get topic detail: %v", err)
	}
	defer resp.Body.Close()
	var detail brokerpkg.TopicDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode topic detail: %v", err)
	}
	return detail
}

func fetchMessages(t *testing.T, baseURL, topic string, partition int) []map[string]interface{} {
	t.Helper()
	url := fmt.Sprintf("%s/api/topics/%s/partitions/%d/messages", baseURL, topic, partition)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get messages: %v", err)
	}
	defer resp.Body.Close()
	var msgs []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	return msgs
}
