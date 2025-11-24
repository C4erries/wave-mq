package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/pkg/api"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Handler struct {
	b   *broker.Broker
	cfg api.BrokerConfig
}

// New returns an HTTP handler for admin JSON API under /api.
func New(b *broker.Broker, cfg api.BrokerConfig) *Handler {
	return &Handler{b: b, cfg: cfg}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/broker", h.handleBroker)
	mux.HandleFunc("/api/summary", h.handleSummary)
	mux.HandleFunc("/api/topics", h.handleTopics)
	mux.HandleFunc("/api/consumers", h.handleConsumers)
	// Dynamic paths handled inside handleTopicPaths.
	mux.HandleFunc("/api/topics/", h.handleTopicPaths)
}

func (h *Handler) handleBroker(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"id":                h.cfg.BrokerID,
		"binaryEndpoint":    h.cfg.BinaryAddr,
		"mqttEndpoint":      h.cfg.MQTTAddr,
		"httpEndpoint":      h.cfg.HTTPAddr,
		"clusterID":         h.cfg.ClusterID,
		"replicationFactor": h.cfg.ReplicationFactor,
	}
	writeJSON(w, resp)
}

func (h *Handler) handleSummary(w http.ResponseWriter, r *http.Request) {
	topics, partitions := h.b.TopicAndPartitionCounts()
	produced := sumCounter("wavemq_messages_produced_total")
	consumed := sumCounter("wavemq_messages_consumed_total")
	errors := sumCounter("wavemq_request_errors_total")
	resp := map[string]interface{}{
		"topics":     topics,
		"partitions": partitions,
		"produced":   produced,
		"consumed":   consumed,
		"errors":     errors,
	}
	writeJSON(w, resp)
}

func (h *Handler) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/topics" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	topics := h.b.TopicsSnapshot()
	writeJSON(w, topics)
}

func (h *Handler) handleTopicPaths(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/topics/"), "/")
	if len(parts) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	name := parts[0]
	if len(parts) == 1 {
		h.topicDetail(w, r, name)
		return
	}
	if len(parts) == 3 && parts[1] == "partitions" && parts[2] != "" && strings.HasSuffix(r.URL.Path, "/messages") {
		// Will be handled by next branch.
	}
	if len(parts) == 4 && parts[1] == "partitions" && parts[3] == "messages" {
		pid, err := strconv.Atoi(parts[2])
		if err != nil {
			http.Error(w, "invalid partition id", http.StatusBadRequest)
			return
		}
		h.partitionMessages(w, r, name, pid)
		return
	}
	http.NotFound(w, r)
}

func (h *Handler) topicDetail(w http.ResponseWriter, r *http.Request, name string) {
	detail, ok := h.b.TopicDetail(name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, detail)
}

func (h *Handler) partitionMessages(w http.ResponseWriter, r *http.Request, topic string, partition int) {
	q := r.URL.Query()
	limit := 50
	if l := q.Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		} else {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
	}
	offsetParam := q.Get("offset")
	_, msgs, err := h.b.FetchMessages(r.Context(), topic, partition, offsetParam, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var resp []map[string]interface{}
	for _, m := range msgs {
		resp = append(resp, map[string]interface{}{
			"partition": partition,
			"offset":    int64(m.Offset),
			"key":       encodeMaybeBase64(m.Key),
			"value":     encodeMaybeBase64(m.Value),
			"timestamp": m.Timestamp.UTC().Format(time.RFC3339),
		})
	}
	// Sort descending by offset.
	sort.Slice(resp, func(i, j int) bool {
		oi := resp[i]["offset"].(int64)
		oj := resp[j]["offset"].(int64)
		return oi > oj
	})
	writeJSON(w, resp)
}

func (h *Handler) handleConsumers(w http.ResponseWriter, r *http.Request) {
	groups := h.b.ConsumerGroupsSnapshot(r.Context())
	writeJSON(w, groups)
}

func encodeMaybeBase64(b []byte) interface{} {
	if len(b) == 0 {
		return ""
	}
	if utf8.Valid(b) {
		return string(b)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func sumCounter(metricName string) float64 {
	var total float64
	metricCh := make(chan prometheus.Metric, 10)
	go func() {
		switch metricName {
		case "wavemq_messages_produced_total":
			observability.MessagesProduced.Collect(metricCh)
		case "wavemq_messages_consumed_total":
			observability.MessagesConsumed.Collect(metricCh)
		case "wavemq_request_errors_total":
			observability.RequestErrors.Collect(metricCh)
		}
		close(metricCh)
	}()
	for m := range metricCh {
		var dtoMetric dto.Metric
		if err := m.Write(&dtoMetric); err == nil && dtoMetric.Counter != nil {
			total += dtoMetric.Counter.GetValue()
		}
	}
	return total
}
