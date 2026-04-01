package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
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
	b          *broker.Broker
	cfg        api.BrokerConfig
	ctrl       controller.MetadataStore
	httpClient HTTPDoer
}

const forwardedCreateTopicHeader = "X-WaveMQ-Forwarded"
const recordContentTypeHeader = "content-type"

type topicProduceByKeyRequest struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	ContentType string `json:"contentType,omitempty"`
}

type partitionProduceRequest struct {
	Key         *string `json:"key"`
	Value       string  `json:"value"`
	ContentType string  `json:"contentType,omitempty"`
}

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// New returns an HTTP handler for admin JSON API under /api.
func New(b *broker.Broker, cfg api.BrokerConfig, ctrl controller.MetadataStore) *Handler {
	return NewWithHTTPClient(b, cfg, ctrl, http.DefaultClient)
}

// NewWithHTTPClient is like New, but allows custom HTTP transport for forwarding requests.
func NewWithHTTPClient(b *broker.Broker, cfg api.BrokerConfig, ctrl controller.MetadataStore, httpClient HTTPDoer) *Handler {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &Handler{b: b, cfg: cfg, ctrl: ctrl, httpClient: httpClient}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("/api/broker", withCORS(http.HandlerFunc(h.handleBroker)))
	mux.Handle("/api/summary", withCORS(http.HandlerFunc(h.handleSummary)))
	mux.Handle("/api/topics", withCORS(http.HandlerFunc(h.handleTopics)))
	mux.Handle("/api/consumers", withCORS(http.HandlerFunc(h.handleConsumers)))
	mux.Handle("/api/consumers/", withCORS(http.HandlerFunc(h.handleConsumerPaths)))
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
	produced := h.b.ProducedMessagesCount()
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

	if len(parts) == 2 && parts[1] == "messages" {
		h.topicMessages(w, r, name)
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

func (h *Handler) topicMessages(w http.ResponseWriter, r *http.Request, topic string) {
	switch r.Method {
	case http.MethodGet:
		h.topicMessagesAll(w, r, topic)
	case http.MethodPost:
		h.topicProduceByKey(w, r, topic)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) topicProduceByKey(w http.ResponseWriter, r *http.Request, topic string) {
	var req topicProduceByKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.Value == "" {
		http.Error(w, "value is required", http.StatusBadRequest)
		return
	}

	key := strings.TrimSpace(req.Key)
	if key == "" {
		http.Error(w, "key is required for hash routing", http.StatusBadRequest)
		return
	}

	value, err := decodeMaybeBase64(req.Value)
	if err != nil {
		http.Error(w, "invalid value encoding", http.StatusBadRequest)
		return
	}

	rec := api.Record{
		Timestamp: time.Now().UTC(),
		Key:       []byte(key),
		Value:     value,
	}
	applyContentType(&rec, req.ContentType)

	partition, base, err := h.b.ProduceByKey(r.Context(), topic, []byte(key), []api.Record{rec})
	if err != nil {
		var nle broker.NotLeaderError
		switch {
		case errors.As(err, &nle):
			if status, payload, ok := h.forwardTopicProduceToLeader(r, topic, req, nle.Leader); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write(payload) // #nosec G705 -- payload is trusted JSON response from peer broker.

				return
			}

			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]interface{}{
				"error":          "not_leader",
				"leaderBrokerID": nle.Leader,
				"topic":          topic,
				"partition":      nle.Partition,
			})

			return
		case errors.Is(err, broker.ErrTopicNotFound):
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

func (h *Handler) topicMessagesAll(w http.ResponseWriter, r *http.Request, topic string) {
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

	partitions, err := h.topicPartitionIDs(r.Context(), topic)
	if err != nil {
		if errors.Is(err, broker.ErrTopicNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	all := make([]map[string]interface{}, 0, limit*len(partitions))
	for _, pid := range partitions {
		_, msgs, err := h.b.FetchMessages(r.Context(), topic, pid, offsetParam, limit)
		if err != nil {
			var nle broker.NotLeaderError
			if errors.As(err, &nle) {
				if status, payload, ok := h.forwardPartitionMessagesToLeader(r, topic, pid, nle.Leader); ok {
					if status != http.StatusOK {
						http.Error(w, "leader fetch failed", http.StatusBadGateway)
						return
					}

					var forwarded []map[string]interface{}
					if err := json.Unmarshal(payload, &forwarded); err != nil {
						http.Error(w, "invalid leader payload", http.StatusBadGateway)
						return
					}

					all = append(all, forwarded...)

					continue
				}
			}

			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		all = append(all, recordsResponse(pid, msgs)...)
	}

	sort.Slice(all, func(i, j int) bool {
		ti := fmt.Sprint(all[i]["timestamp"])

		tj := fmt.Sprint(all[j]["timestamp"])
		if ti != tj {
			return ti > tj
		}

		return offsetInt64(all[i]["offset"]) > offsetInt64(all[j]["offset"])
	})

	if len(all) > limit {
		all = all[:limit]
	}

	writeJSON(w, all)
}

func (h *Handler) topicPartitionIDs(ctx context.Context, topic string) ([]int, error) {
	ids := make(map[int]struct{})

	if h.ctrl != nil {
		meta, err := h.ctrl.GetClusterMetadata(ctx)
		if err == nil {
			for _, p := range meta.Partitions {
				if p.Topic == topic {
					ids[p.Partition] = struct{}{}
				}
			}
		}
	}

	if len(ids) == 0 {
		detail, ok := h.b.TopicDetail(topic)
		if !ok {
			return nil, fmt.Errorf("%w", broker.ErrTopicNotFound)
		}

		for _, p := range detail.Partitions {
			ids[p.ID] = struct{}{}
		}
	}

	if len(ids) == 0 {
		return nil, fmt.Errorf("%w", broker.ErrTopicNotFound)
	}

	partitions := make([]int, 0, len(ids))
	for pid := range ids {
		partitions = append(partitions, pid)
	}

	sort.Ints(partitions)

	return partitions, nil
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

	resp := recordsResponse(partition, msgs)
	// Sort descending by offset.
	sort.Slice(resp, func(i, j int) bool {
		return offsetInt64(resp[i]["offset"]) > offsetInt64(resp[j]["offset"])
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

func (h *Handler) handleConsumerPaths(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/consumers/"), "/")
	if len(parts) != 6 || parts[1] != "topics" || parts[3] != "partitions" || parts[5] != "offset" {
		http.NotFound(w, r)
		return
	}

	group := parts[0]

	topic := parts[2]
	if group == "" || topic == "" {
		http.NotFound(w, r)
		return
	}

	partition, err := strconv.Atoi(parts[4])
	if err != nil {
		http.Error(w, "invalid partition id", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleConsumerCommittedOffset(w, r, group, topic, partition)
	case http.MethodPost:
		h.handleConsumerCommitOffset(w, r, group, topic, partition)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleConsumerCommittedOffset(w http.ResponseWriter, r *http.Request, group, topic string, partition int) {
	if !h.consumerGroupExists(r.Context(), group) {
		http.NotFound(w, r)
		return
	}

	if !h.topicPartitionExists(topic, partition) {
		http.NotFound(w, r)
		return
	}

	offset, err := h.b.FetchCommitted(r.Context(), group, topic, partition)
	if err != nil {
		if h.consumerOffsetNotFound(err) {
			http.NotFound(w, r)
			return
		}

		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	writeJSON(w, consumerOffsetResponse{
		Group:     group,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	})
}

func (h *Handler) handleConsumerCommitOffset(w http.ResponseWriter, r *http.Request, group, topic string, partition int) {
	if !h.topicPartitionExists(topic, partition) {
		http.NotFound(w, r)
		return
	}

	var req struct {
		Offset api.Offset `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := h.b.CommitOffset(r.Context(), group, topic, partition, req.Offset); err != nil {
		if h.consumerOffsetNotFound(err) {
			http.NotFound(w, r)
			return
		}

		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	writeJSON(w, consumerOffsetResponse{
		Group:     group,
		Topic:     topic,
		Partition: partition,
		Offset:    req.Offset,
	})
}

type consumerOffsetResponse struct {
	Group     string     `json:"group"`
	Topic     string     `json:"topic"`
	Partition int        `json:"partition"`
	Offset    api.Offset `json:"offset"`
}

func (h *Handler) consumerGroupExists(ctx context.Context, group string) bool {
	for _, info := range h.b.ConsumerGroupsSnapshot(ctx) {
		if info.Name == group {
			return true
		}
	}

	return false
}

func (h *Handler) topicPartitionExists(topic string, partition int) bool {
	detail, ok := h.b.TopicDetail(topic)
	if !ok {
		return false
	}

	for _, p := range detail.Partitions {
		if p.ID == partition {
			return true
		}
	}

	return false
}

func (h *Handler) consumerOffsetNotFound(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "group not found") || strings.Contains(msg, "topic not found") || strings.Contains(msg, "partition not found")
}

func (h *Handler) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string `json:"name"`
		Partitions        int    `json:"partitions"`
		ReplicationFactor int    `json:"replicationFactor"`
		RetentionBytes    int64  `json:"retentionBytes,omitempty"`
		RetentionHours    int    `json:"retentionHours,omitempty"`
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
		RetentionBytes:    req.RetentionBytes,
	}
	if req.RetentionHours > 0 {
		cfg.RetentionTime = time.Duration(req.RetentionHours) * time.Hour
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
		RetentionBytes    int64  `json:"retentionBytes,omitempty"`
		RetentionHours    int    `json:"retentionHours,omitempty"`
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

		status, payload, ok := h.forwardToURL(r, http.MethodPost, leaderURL, data)
		if !ok {
			currentLeader = ""
			continue
		}

		if status == http.StatusConflict {
			if nextLeader, ok := parseNotLeaderHint(payload); ok && nextLeader != "" && nextLeader != currentLeader {
				currentLeader = nextLeader
				continue
			}
		}

		return status, payload, true
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

	var req partitionProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.Value == "" {
		http.Error(w, "value is required", http.StatusBadRequest)
		return
	}

	value, err := decodeMaybeBase64(req.Value)
	if err != nil {
		http.Error(w, "invalid value encoding", http.StatusBadRequest)
		return
	}

	rec := api.Record{
		Timestamp: time.Now().UTC(),
		Value:     value,
	}
	if req.Key != nil {
		key, err := decodeMaybeBase64(*req.Key)
		if err != nil {
			http.Error(w, "invalid key encoding", http.StatusBadRequest)
			return
		}
		rec.Key = key
	}
	applyContentType(&rec, req.ContentType)

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

func recordsResponse(partition int, msgs []api.Record) []map[string]interface{} {
	resp := make([]map[string]interface{}, 0, len(msgs))
	for _, m := range msgs {
		record := map[string]interface{}{
			"partition": partition,
			"offset":    int64(m.Offset),
			"key":       encodeMaybeBase64(m.Key),
			"value":     encodeRecordValue(m.Value, m.Headers),
			"timestamp": m.Timestamp.UTC().Format(time.RFC3339),
		}
		if contentType, ok := recordContentType(m.Headers); ok {
			record["contentType"] = contentType
		}

		resp = append(resp, record)
	}

	return resp
}

func offsetInt64(v interface{}) int64 {
	switch off := v.(type) {
	case int:
		return int64(off)
	case int64:
		return off
	case float64:
		return int64(off)
	default:
		return 0
	}
}

func (h *Handler) forwardPartitionMessagesToLeader(
	r *http.Request,
	topic string,
	partition int,
	leaderBrokerID int,
) (int, []byte, bool) {
	if r.Header.Get(forwardedCreateTopicHeader) != "" {
		return 0, nil, false
	}

	baseURL, ok := h.resolveTopicsAPIByBrokerID(r.Context(), leaderBrokerID)
	if !ok {
		return 0, nil, false
	}

	u := baseURL + "/" + url.PathEscape(topic) + "/partitions/" + strconv.Itoa(partition) + "/messages"
	if raw := r.URL.RawQuery; raw != "" {
		u += "?" + raw
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, http.NoBody)
	if err != nil {
		return 0, nil, false
	}

	return h.forwardRequest(req)
}

func (h *Handler) forwardTopicProduceToLeader(r *http.Request, topic string, payload topicProduceByKeyRequest, leaderBrokerID int) (int, []byte, bool) {
	if r.Header.Get(forwardedCreateTopicHeader) != "" {
		return 0, nil, false
	}

	baseURL, ok := h.resolveTopicsAPIByBrokerID(r.Context(), leaderBrokerID)
	if !ok {
		return 0, nil, false
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, false
	}

	u := baseURL + "/" + url.PathEscape(topic) + "/messages"

	return h.forwardToURL(r, http.MethodPost, u, data)
}

func (h *Handler) resolveTopicsAPIByBrokerID(ctx context.Context, brokerID int) (string, bool) {
	if brokerID == 0 || h.ctrl == nil {
		return "", false
	}

	meta, err := h.ctrl.GetClusterMetadata(ctx)
	if err != nil {
		return "", false
	}

	for _, info := range meta.Brokers {
		if info.BrokerID != brokerID {
			continue
		}

		httpAddr, ok := normalizeHostPort(info.HTTPAddr)
		if !ok {
			return "", false
		}

		return "http://" + httpAddr + "/api/topics", true
	}

	return "", false
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

func encodeRecordValue(b []byte, headers []api.Header) interface{} {
	contentType, ok := recordContentType(headers)
	if !ok {
		return encodeMaybeBase64(b)
	}

	if len(b) == 0 {
		return ""
	}

	if isTextContentType(contentType) && utf8.Valid(b) {
		return string(b)
	}

	return "base64:" + base64.StdEncoding.EncodeToString(b)
}

func recordContentType(headers []api.Header) (string, bool) {
	for _, header := range headers {
		if strings.EqualFold(header.Key, recordContentTypeHeader) {
			value := strings.TrimSpace(string(header.Value))
			if value == "" {
				return "", false
			}
			return value, true
		}
	}

	return "", false
}

func applyContentType(rec *api.Record, contentType string) {
	value := strings.TrimSpace(contentType)
	if value == "" {
		return
	}

	rec.Headers = append(rec.Headers, api.Header{
		Key:   recordContentTypeHeader,
		Value: []byte(value),
	})
}

func decodeMaybeBase64(raw string) ([]byte, error) {
	if strings.HasPrefix(raw, "base64:") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(raw, "base64:"))
		if err != nil {
			return nil, err
		}
		return decoded, nil
	}

	return []byte(raw), nil
}

func isTextContentType(contentType string) bool {
	trimmed := strings.TrimSpace(contentType)
	if trimmed == "" {
		return false
	}

	mediaType, _, err := mime.ParseMediaType(trimmed)
	if err != nil {
		mediaType = trimmed
	}

	mediaType = strings.ToLower(mediaType)
	return strings.HasPrefix(mediaType, "text/") ||
		mediaType == "application/json" ||
		strings.HasSuffix(mediaType, "+json")
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) forwardToURL(r *http.Request, method, target string, body []byte) (int, []byte, bool) {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(r.Context(), method, target, reader)
	if err != nil {
		return 0, nil, false
	}

	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set(forwardedCreateTopicHeader, "1")

	return h.forwardRequest(req)
}

func (h *Handler) forwardRequest(req *http.Request) (int, []byte, bool) {
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return 0, nil, false
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, false
	}

	return resp.StatusCode, payload, true
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

	var collector prometheus.Collector

	switch metricName {
	case "wavemq_messages_produced_total":
		collector = observability.MessagesProduced
	case "wavemq_messages_consumed_total":
		collector = observability.MessagesConsumed
	case "wavemq_request_errors_total":
		collector = observability.RequestErrors
	default:
		return 0
	}

	metricCh := make(chan prometheus.Metric, 32)
	collector.Collect(metricCh)
	close(metricCh)

	for m := range metricCh {
		var dtoMetric dto.Metric
		if err := m.Write(&dtoMetric); err == nil && dtoMetric.Counter != nil {
			total += dtoMetric.Counter.GetValue()
		}
	}

	return total
}
