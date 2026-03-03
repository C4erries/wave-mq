package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/pkg/api"
)

type Handler struct {
	b    *broker.Broker
	cfg  api.BrokerConfig
	ctrl controller.MetadataStore
}

const forwardedCreateTopicHeader = "X-WaveMQ-Forwarded"

// New returns an HTTP handler for admin JSON API under /api.
func New(b *broker.Broker, cfg api.BrokerConfig, ctrl controller.MetadataStore) *Handler {
	return &Handler{b: b, cfg: cfg, ctrl: ctrl}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("/api/broker", withCORS(http.HandlerFunc(h.handleBroker)))
	mux.Handle("/api/summary", withCORS(http.HandlerFunc(h.handleSummary)))
	mux.Handle("/api/topics", withCORS(http.HandlerFunc(h.handleTopics)))
	mux.Handle("/api/consumers", withCORS(http.HandlerFunc(h.handleConsumers)))
	mux.Handle("/api/cluster", withCORS(http.HandlerFunc(h.handleClusterMetadata)))
	mux.Handle("/api/controller", withCORS(http.HandlerFunc(h.handleControllerStatus)))
	mux.Handle("/api/controller/brokers", withCORS(http.HandlerFunc(h.handleControllerRegisterBroker)))
	mux.Handle("/api/topics/", withCORS(http.HandlerFunc(h.handleTopicPaths)))
}

func (h *Handler) handleBroker(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]interface{}{
		"id":                h.cfg.BrokerID,
		"binaryEndpoint":    h.cfg.BinaryAddr,
		"mqttEndpoint":      h.cfg.MQTTAddr,
		"httpEndpoint":      h.cfg.HTTPAddr,
		"clusterID":         h.cfg.ClusterID,
		"replicationFactor": h.cfg.ReplicationFactor,
		"controllerMode":    h.cfg.ControllerMode,
	}
	writeJSON(w, resp)
}

func (h *Handler) handleControllerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	mode := h.cfg.ControllerMode
	if mode == "" {
		mode = "single"
	}

	raftState := "none"
	term := uint64(0)
	peers := []controller.PeerInfo{}
	leader := ""
	leaderID := ""
	meta := api.ClusterMetadata{}

	if h.ctrl != nil {
		if rc, ok := h.ctrl.(interface {
			ControllerMode() string
			RaftState() string
			RaftTerm() uint64
			RaftPeers() []controller.PeerInfo
		}); ok {
			mode = rc.ControllerMode()
			raftState = strings.ToLower(rc.RaftState())
			term = rc.RaftTerm()
			peers = rc.RaftPeers()
		}

		if rl, ok := h.ctrl.(interface {
			RaftLeader() string
		}); ok {
			leader = rl.RaftLeader()
		}

		if rl, ok := h.ctrl.(interface {
			RaftLeaderID() string
		}); ok {
			leaderID = rl.RaftLeaderID()
		}

		meta, _ = h.ctrl.GetClusterMetadata(r.Context())
	}

	resp := map[string]interface{}{
		"mode":      mode,
		"raftState": raftState,
		"term":      term,
		"peers":     peers,
		"leader":    leader,
		"leaderID":  leaderID,
		"clusterID": meta.ClusterID,
		"version":   meta.Version,
	}
	writeJSON(w, resp)
}

func (h *Handler) handleControllerRegisterBroker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if h.ctrl == nil {
		http.Error(w, "controller not configured", http.StatusInternalServerError)
		return
	}

	var req struct {
		BrokerID       int    `json:"brokerID"`
		Host           string `json:"host"`
		Port           int    `json:"port,omitempty"`
		HTTPAddr       string `json:"httpAddr,omitempty"`
		ControllerAddr string `json:"controllerAddr,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.BrokerID == 0 || req.Host == "" {
		http.Error(w, "brokerID and host required", http.StatusBadRequest)
		return
	}

	if h.cfg.ControllerMode == "raft" && (req.HTTPAddr == "" || req.ControllerAddr == "") {
		http.Error(w, "httpAddr and controllerAddr required in raft mode", http.StatusBadRequest)
		return
	}

	info := api.BrokerInfo{
		BrokerID:       req.BrokerID,
		Host:           req.Host,
		Port:           req.Port,
		HTTPAddr:       req.HTTPAddr,
		ControllerAddr: req.ControllerAddr,
	}
	if err := h.ctrl.RegisterBroker(r.Context(), info); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleClusterMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if h.ctrl == nil {
		http.Error(w, "controller not configured", http.StatusInternalServerError)
		return
	}

	meta, err := h.ctrl.GetClusterMetadata(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, meta)
}

func (h *Handler) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	topics, partitions := h.b.TopicAndPartitionCounts()
	produced := sumCounter("wavemq_messages_produced_total")
	consumed := sumCounter("wavemq_messages_consumed_total")
	reqErrors := sumCounter("wavemq_request_errors_total")
	resp := map[string]interface{}{
		"topics":     topics,
		"partitions": partitions,
		"produced":   produced,
		"consumed":   consumed,
		"errors":     reqErrors,
	}
	writeJSON(w, resp)
}

func (h *Handler) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/topics" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		topics := h.b.TopicsSnapshot()
		writeJSON(w, topics)
	case http.MethodPost:
		h.handleCreateTopic(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
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

	if len(parts) == 4 && parts[1] == "partitions" && parts[3] == "messages" {
		pid, err := strconv.Atoi(parts[2])
		if err != nil {
			http.Error(w, "invalid partition id", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			h.partitionMessages(w, r, name, pid)
		case http.MethodPost:
			h.partitionProduce(w, r, name, pid)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}

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
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

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
		var nle broker.NotLeaderError
		switch {
		case errors.As(err, &nle):
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]interface{}{
				"error":          "not_leader",
				"leaderBrokerID": nle.Leader,
				"topic":          topic,
				"partition":      partition,
			})

			return
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	groups := h.b.ConsumerGroupsSnapshot(r.Context())
	writeJSON(w, groups)
}

func (h *Handler) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string `json:"name"`
		Partitions        int    `json:"partitions"`
		ReplicationFactor int    `json:"replicationFactor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.Partitions < 1 {
		http.Error(w, "name required and partitions>=1", http.StatusBadRequest)
		return
	}

	cfg := api.TopicConfig{
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
	}

	ctx := r.Context()
	if err := h.b.CreateTopic(ctx, req.Name, cfg); err != nil {
		var nle controller.NotLeaderError

		switch {
		case errors.As(err, &nle):
			if status, payload, ok := h.forwardCreateTopicToLeader(r, req, nle.Leader); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write(payload) // #nosec G705 -- payload is leader JSON API response in application/json.

				return
			}

			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]interface{}{
				"error":   "not_leader",
				"leader":  nle.Leader,
				"message": err.Error(),
			})

			return
		case errors.Is(err, controller.ErrLeaderNotElected):
			w.WriteHeader(http.StatusServiceUnavailable)
			writeJSON(w, map[string]interface{}{
				"error":   "leader_not_elected",
				"message": err.Error(),
			})

			return
		case errors.Is(err, broker.ErrTopicExists):
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]interface{}{
				"error":   "topic_exists",
				"message": err.Error(),
			})

			return
		}

		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]interface{}{
			"error":   "bad_request",
			"message": err.Error(),
		})

		return
	}

	detail, _ := h.b.TopicDetail(req.Name)

	w.WriteHeader(http.StatusCreated)
	writeJSON(w, detail)
}

func (h *Handler) forwardCreateTopicToLeader(
	r *http.Request,
	req struct {
		Name              string `json:"name"`
		Partitions        int    `json:"partitions"`
		ReplicationFactor int    `json:"replicationFactor"`
	},
	leaderHint string,
) (int, []byte, bool) {
	if r.Header.Get(forwardedCreateTopicHeader) != "" {
		return 0, nil, false
	}

	data, err := json.Marshal(req)
	if err != nil {
		return 0, nil, false
	}

	currentLeader := leaderHint

	for attempt := 0; attempt < 3; attempt++ {
		leaderURL, leaderAddr, ok := h.leaderTopicsURL(r.Context(), currentLeader)
		if !ok {
			return 0, nil, false
		}

		currentLeader = leaderAddr

		httpReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, leaderURL, bytes.NewReader(data))
		if err != nil {
			return 0, nil, false
		}

		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set(forwardedCreateTopicHeader, "1")

		resp, err := http.DefaultClient.Do(httpReq) // #nosec G704 -- leader host comes from local Raft state.
		if err != nil {
			currentLeader = ""
			continue
		}

		payload, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if readErr != nil {
			currentLeader = ""
			continue
		}

		if resp.StatusCode == http.StatusConflict {
			if nextLeader, ok := parseNotLeaderHint(payload); ok && nextLeader != "" && nextLeader != currentLeader {
				currentLeader = nextLeader
				continue
			}
		}

		return resp.StatusCode, payload, true
	}

	return 0, nil, false
}

func (h *Handler) leaderTopicsURL(ctx context.Context, leaderHint string) (string, string, bool) {
	if h.cfg.ControllerMode != "raft" || h.ctrl == nil {
		return "", "", false
	}

	rs, ok := h.ctrl.(interface {
		RaftState() string
		RaftLeader() string
	})
	if !ok {
		return "", "", false
	}

	if strings.ToLower(rs.RaftState()) == "leader" {
		return "", "", false
	}

	leaderAddr := leaderHint
	if leaderAddr == "" {
		if rl, ok := h.ctrl.(interface{ RaftLeaderID() string }); ok {
			leaderAddr = rl.RaftLeaderID()
		}
	}

	if leaderAddr == "" {
		leaderAddr = rs.RaftLeader()
	}

	if leaderAddr == "" {
		return "", "", false
	}

	meta, err := h.ctrl.GetClusterMetadata(ctx)
	if err != nil {
		return "", "", false
	}

	info, ok := brokerInfoForLeader(meta.Brokers, leaderAddr)
	if !ok {
		return "", "", false
	}

	httpAddr, ok := normalizeHostPort(info.HTTPAddr)
	if !ok {
		return "", "", false
	}

	return "http://" + httpAddr + "/api/topics", leaderAddr, true
}

func brokerInfoForLeader(brokers []api.BrokerInfo, leaderAddr string) (api.BrokerInfo, bool) {
	for _, brokerInfo := range brokers {
		if brokerInfo.ControllerAddr == leaderAddr {
			return brokerInfo, true
		}
	}

	leaderHost, ok := hostFromAddress(leaderAddr)
	if !ok {
		return api.BrokerInfo{}, false
	}

	for _, brokerInfo := range brokers {
		for _, candidate := range []string{brokerInfo.ControllerAddr, brokerInfo.HTTPAddr, brokerInfo.Host} {
			host, ok := hostFromAddress(candidate)
			if !ok {
				continue
			}

			if host == leaderHost {
				return brokerInfo, true
			}
		}
	}

	return api.BrokerInfo{}, false
}

func normalizeHostPort(raw string) (string, bool) {
	addr := strings.TrimSpace(raw)
	if addr == "" {
		return "", false
	}

	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil || u.Host == "" {
			return "", false
		}

		addr = u.Host
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", false
	}

	return net.JoinHostPort(host, port), true
}

func hostFromAddress(addr string) (string, bool) {
	value := strings.TrimSpace(addr)
	if value == "" || strings.HasPrefix(value, ":") {
		return "", false
	}

	if strings.Contains(value, "://") {
		u, err := url.Parse(value)
		if err != nil || u.Hostname() == "" {
			return "", false
		}

		return u.Hostname(), true
	}

	host, _, err := net.SplitHostPort(value)
	if err == nil && host != "" {
		return host, true
	}

	return value, true
}

func parseNotLeaderHint(payload []byte) (string, bool) {
	var body struct {
		Error  string `json:"error"`
		Leader string `json:"leader"`
	}

	if err := json.Unmarshal(payload, &body); err != nil {
		return "", false
	}

	if body.Error != "not_leader" || body.Leader == "" {
		return "", false
	}

	return body.Leader, true
}

func (h *Handler) partitionProduce(w http.ResponseWriter, r *http.Request, topic string, partition int) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key   *string `json:"key"`
		Value string  `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.Value == "" {
		http.Error(w, "value is required", http.StatusBadRequest)
		return
	}

	rec := api.Record{
		Timestamp: time.Now().UTC(),
		Value:     []byte(req.Value),
	}
	if req.Key != nil {
		rec.Key = []byte(*req.Key)
	}

	base, err := h.b.Produce(r.Context(), topic, partition, []api.Record{rec})
	if err != nil {
		switch {
		case errors.As(err, &broker.NotLeaderError{}):
			var nle broker.NotLeaderError

			_ = errors.As(err, &nle)

			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]interface{}{
				"error":          "not_leader",
				"leaderBrokerID": nle.Leader,
				"topic":          topic,
				"partition":      partition,
			})

			return
		case errors.Is(err, broker.ErrTopicNotFound), errors.Is(err, broker.ErrPartitionNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	writeJSON(w, map[string]interface{}{
		"partition":  partition,
		"baseOffset": base,
	})
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

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		h.ServeHTTP(w, r)
	})
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
