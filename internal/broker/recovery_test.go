package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	brokerpkg "github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/httpapi"
	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

func assertClose(t *testing.T, target string, err error) {
	t.Helper()

	if err != nil && !errors.Is(err, os.ErrClosed) {
		t.Errorf("close %s: %v", target, err)
	}
}

func requireClose(t *testing.T, target string, err error) {
	t.Helper()

	if err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close %s: %v", target, err)
	}
}

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

	recoveredTopics, err := metaStore.RecoverTopics(ctx)
	if err != nil {
		t.Fatalf("recover topics: %v", err)
	}

	ctrl, err := controller.NewSingleNodeController(brokerCfg, recoveredTopics.Topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	b, err := brokerpkg.NewBroker(brokerCfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	server := newHTTPTestServer(t, b, brokerCfg, metaStore)
	client := server.Client()

	createTopicHTTP(t, client, server.URL, "alpha", 2)
	createTopicHTTP(t, client, server.URL, "beta", 1)
	produceHTTP(t, client, server.URL, "alpha", 0, "alpha-0")
	produceHTTP(t, client, server.URL, "alpha", 1, "alpha-1")
	produceHTTP(t, client, server.URL, "beta", 0, "beta-0")

	server.Close()
	requireClose(t, "broker before restart", b.Close())
	requireClose(t, "storage before restart", store.Close())
	requireClose(t, "offset store before restart", offsetStore.Close())
	requireClose(t, "metadata store before restart", metaStore.Close())

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

	recoveredTopics, err = metaStore.RecoverTopics(ctx)
	if err != nil {
		t.Fatalf("recover topics restart: %v", err)
	}

	ctrl, err = controller.NewSingleNodeController(brokerCfg, recoveredTopics.Topics)
	if err != nil {
		t.Fatalf("controller restart: %v", err)
	}

	b, err = brokerpkg.NewBroker(brokerCfg, store, offsetStore, metaStore, ctrl, nil)
	if err != nil {
		t.Fatalf("broker reopen: %v", err)
	}

	server = newHTTPTestServer(t, b, brokerCfg, metaStore)

	defer func() {
		server.Close()
		assertClose(t, "broker", b.Close())
		assertClose(t, "storage", store.Close())
		assertClose(t, "offset store", offsetStore.Close())
		assertClose(t, "metadata store", metaStore.Close())
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

	cluster := fetchCluster(t, server.URL)
	if len(cluster.Brokers) != 1 || cluster.Brokers[0].BrokerID != brokerCfg.BrokerID {
		t.Fatalf("unexpected cluster brokers: %+v", cluster.Brokers)
	}

	if len(cluster.Partitions) != 3 {
		t.Fatalf("expected 3 partitions in cluster metadata, got %d", len(cluster.Partitions))
	}
}

func newHTTPTestServer(t *testing.T, b *brokerpkg.Broker, cfg api.BrokerConfig, metaStore *metadata.Store) *httptest.Server {
	t.Helper()

	recovered, err := metaStore.RecoverTopics(context.Background())
	if err != nil {
		t.Fatalf("recover topics for controller: %v", err)
	}

	ctrl, err := controller.NewSingleNodeController(cfg, recovered.Topics)
	if err != nil {
		t.Fatalf("controller: %v", err)
	}

	handler := httpapi.New(b, cfg, ctrl)
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

	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close create topic response body: %v", err)
	}

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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close produce response body: %v", err)
		}
	}()

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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close topics response body: %v", err)
		}
	}()

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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close topic detail response body: %v", err)
		}
	}()

	var detail brokerpkg.TopicDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode topic detail: %v", err)
	}

	return detail
}

func fetchMessages(t *testing.T, baseURL, topic string, partition int) []map[string]interface{} {
	t.Helper()

	url := fmt.Sprintf("%s/api/topics/%s/partitions/%d/messages", baseURL, topic, partition)

	resp, err := http.Get(url) // #nosec G107 -- url points to local httptest server.
	if err != nil {
		t.Fatalf("get messages: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close messages response body: %v", err)
		}
	}()

	var msgs []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode messages: %v", err)
	}

	return msgs
}

func fetchCluster(t *testing.T, baseURL string) api.ClusterMetadata {
	t.Helper()

	resp, err := http.Get(baseURL + "/api/cluster")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close cluster response body: %v", err)
		}
	}()

	var meta api.ClusterMetadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatalf("decode cluster: %v", err)
	}

	return meta
}
