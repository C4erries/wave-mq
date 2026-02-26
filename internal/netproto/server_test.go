package netproto

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeBroker struct {
	topics    map[string]map[int][]api.Record
	committed map[string]api.Offset
	defaultRF int
	lastCfg   api.TopicConfig
	notLeader map[string]struct{}
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		topics:    make(map[string]map[int][]api.Record),
		committed: make(map[string]api.Offset),
		defaultRF: 1,
		notLeader: make(map[string]struct{}),
	}
}

func (b *fakeBroker) CreateTopic(ctx context.Context, name string, cfg api.TopicConfig) error {
	_ = ctx

	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = b.defaultRF
	}

	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}

	b.lastCfg = cfg
	if _, ok := b.topics[name]; ok {
		return nil
	}

	parts := make(map[int][]api.Record, cfg.Partitions)
	for i := 0; i < cfg.Partitions; i++ {
		parts[i] = []api.Record{}
	}

	b.topics[name] = parts

	return nil
}

func (b *fakeBroker) Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error) {
	_ = ctx

	if _, ok := b.notLeader[fmt.Sprintf("%s:%d", topic, partition)]; ok {
		return -1, broker.NotLeaderError{Topic: topic, Partition: partition, Leader: 99}
	}

	t, ok := b.topics[topic]
	if !ok {
		return -1, errTopicNotFound
	}

	recs, ok := t[partition]
	if !ok {
		return -1, errPartitionNotFound
	}

	base := api.Offset(len(recs))
	for i := range records {
		records[i].Offset = base + api.Offset(i)
		if records[i].Timestamp.IsZero() {
			records[i].Timestamp = time.Now()
		}

		recs = append(recs, records[i])
	}

	t[partition] = recs

	return base, nil
}

func (b *fakeBroker) Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	_ = ctx
	_ = maxBytes

	if _, ok := b.notLeader[fmt.Sprintf("%s:%d", topic, partition)]; ok {
		return nil, broker.NotLeaderError{Topic: topic, Partition: partition, Leader: 99}
	}

	t, ok := b.topics[topic]
	if !ok {
		return nil, errTopicNotFound
	}

	recs, ok := t[partition]
	if !ok {
		return nil, errPartitionNotFound
	}

	if offset >= api.Offset(len(recs)) {
		return []api.Record{}, nil
	}

	return recs[offset:], nil
}

func (b *fakeBroker) ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error) {
	_ = ctx
	_ = topic
	_ = partition

	return 0, 0, nil
}

func (b *fakeBroker) CommitOffset(ctx context.Context, group string, topic string, partition int, offset api.Offset) error {
	_ = ctx
	key := fmt.Sprintf("%s:%s:%d", group, topic, partition)

	current, ok := b.committed[key]
	if ok && offset < current {
		return fmt.Errorf("regression")
	}

	b.committed[key] = offset

	return nil
}

func (b *fakeBroker) FetchCommitted(ctx context.Context, group string, topic string, partition int) (api.Offset, error) {
	_ = ctx
	key := fmt.Sprintf("%s:%s:%d", group, topic, partition)

	off, ok := b.committed[key]
	if !ok {
		return -1, errPartitionNotFound
	}

	return off, nil
}

func (b *fakeBroker) Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error) {
	_ = ctx

	var res []api.PartitionMetadata

	for topic, parts := range b.topics {
		if len(topics) > 0 && topics[0] != topic {
			continue
		}

		for pid := range parts {
			res = append(res, api.PartitionMetadata{
				Replica: api.PartitionReplica{
					Topic:     topic,
					Partition: pid,
					BrokerID:  1,
					Role:      api.RoleLeader,
				},
				StartOffset:   0,
				HighWatermark: api.Offset(len(parts[pid])) - 1,
				Leader:        1,
				Replicas:      []int{1},
				ISR:           []int{1},
			})
		}
	}

	return res, nil
}

var (
	errTopicNotFound     = fmt.Errorf("topic not found")
	errPartitionNotFound = fmt.Errorf("partition not found")
)

func TestServerHandlers(t *testing.T) {
	b := newFakeBroker()
	b.defaultRF = 2

	s, err := NewServer("localhost:0", b)
	if err != nil {
		t.Fatalf("server: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go s.handleConnection(serverConn)

	send := func(apiKey api.APIKey, corr int32, payload []byte) ([]byte, error) {
		frame, err := encodeRequestFrame(apiKey, corr, payload)
		if err != nil {
			return nil, err
		}

		if _, err := clientConn.Write(frame); err != nil {
			return nil, err
		}

		_, _, respPayload, err := decodeResponseFrame(clientConn)

		return respPayload, err
	}

	// CreateTopic
	ctReq := &CreateTopicRequest{Topic: "a", Partitions: 1, ReplicationFactor: 3}
	ctPayload, _ := encodeCreateTopicRequest(ctReq)

	respPayload, err := send(api.APIKeyCreateTopic, 1, ctPayload)
	if err != nil {
		t.Fatalf("create-topic send: %v", err)
	}

	ctResp, err := decodeCreateTopicResponse(respPayload)
	if err != nil || ctResp.Error != api.ErrNone {
		t.Fatalf("create-topic resp: %v err=%v", ctResp, err)
	}

	if b.lastCfg.ReplicationFactor != 3 {
		t.Fatalf("expected rf 3 forwarded to broker, got %d", b.lastCfg.ReplicationFactor)
	}

	// Produce
	pReq := &ProduceRequest{Topic: "a", Partition: 0, Records: []api.Record{{Value: []byte("v1")}}}
	pPayload, _ := encodeProduceRequest(pReq)

	pRespPayload, err := send(api.APIKeyProduce, 2, pPayload)
	if err != nil {
		t.Fatalf("produce send: %v", err)
	}

	pResp, err := decodeProduceResponse(pRespPayload)
	if err != nil || pResp.Error != api.ErrNone || pResp.BaseOffset != 0 {
		t.Fatalf("produce resp: %+v err=%v", pResp, err)
	}

	// Fetch
	fReq := &FetchRequest{Topic: "a", Partition: 0, Offset: 0, MaxBytes: 0}
	fPayload, _ := encodeFetchRequest(fReq)

	fRespPayload, err := send(api.APIKeyFetch, 3, fPayload)
	if err != nil {
		t.Fatalf("fetch send: %v", err)
	}

	fResp, err := decodeFetchResponse(fRespPayload)
	if err != nil || fResp.Error != api.ErrNone || len(fResp.Records) != 1 || !bytes.Equal(fResp.Records[0].Value, []byte("v1")) {
		t.Fatalf("fetch resp: %+v err=%v", fResp, err)
	}

	// CommitOffset
	coReq := &CommitOffsetRequest{Group: "g1", Topic: "a", Partition: 0, Offset: 1}
	coPayload, _ := encodeCommitOffsetRequest(coReq)

	coRespPayload, err := send(api.APIKeyCommitOffset, 4, coPayload)
	if err != nil {
		t.Fatalf("commit send: %v", err)
	}

	coResp, err := decodeCommitOffsetResponse(coRespPayload)
	if err != nil || coResp.Error != api.ErrNone {
		t.Fatalf("commit resp: %+v err=%v", coResp, err)
	}

	// FetchCommitted
	fcReq := &FetchCommittedRequest{Group: "g1", Topic: "a", Partition: 0}
	fcPayload, _ := encodeFetchCommittedRequest(fcReq)

	fcRespPayload, err := send(api.APIKeyFetchCommitted, 5, fcPayload)
	if err != nil {
		t.Fatalf("fetch committed send: %v", err)
	}

	fcResp, err := decodeFetchCommittedResponse(fcRespPayload)
	if err != nil || fcResp.Error != api.ErrNone || fcResp.Offset != 1 {
		t.Fatalf("fetch committed resp: %+v err=%v", fcResp, err)
	}

	// NotLeader handling
	b.notLeader["a:0"] = struct{}{}
	pPayload2, _ := encodeProduceRequest(&ProduceRequest{Topic: "a", Partition: 0, Records: []api.Record{{Value: []byte("v2")}}})

	notLeaderPayload, err := send(api.APIKeyProduce, 6, pPayload2)
	if err != nil {
		t.Fatalf("produce notleader send: %v", err)
	}

	pResp2, err := decodeProduceResponse(notLeaderPayload)
	if err != nil || pResp2.Error != api.ErrNotLeader {
		t.Fatalf("expected not leader error, got %+v err=%v", pResp2, err)
	}

	fetchPayload, _ := encodeFetchRequest(&FetchRequest{Topic: "a", Partition: 0, Offset: 0, MaxBytes: 0})

	notLeaderFetchPayload, err := send(api.APIKeyFetch, 7, fetchPayload)
	if err != nil {
		t.Fatalf("fetch notleader send: %v", err)
	}

	fResp2, err := decodeFetchResponse(notLeaderFetchPayload)
	if err != nil || fResp2.Error != api.ErrNotLeader {
		t.Fatalf("expected fetch not leader error, got %+v err=%v", fResp2, err)
	}

	// CreateTopic default RF
	ctReq2 := &CreateTopicRequest{Topic: "b", Partitions: 1, ReplicationFactor: 0}
	ctPayload2, _ := encodeCreateTopicRequest(ctReq2)

	respPayload, err = send(api.APIKeyCreateTopic, 8, ctPayload2)
	if err != nil {
		t.Fatalf("create-topic default send: %v", err)
	}

	ctResp2, err := decodeCreateTopicResponse(respPayload)
	if err != nil || ctResp2.Error != api.ErrNone {
		t.Fatalf("create-topic default resp: %v err=%v", ctResp2, err)
	}

	if b.lastCfg.ReplicationFactor != b.defaultRF {
		t.Fatalf("expected rf fallback %d, got %d", b.defaultRF, b.lastCfg.ReplicationFactor)
	}
}
