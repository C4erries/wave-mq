package netproto

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeBroker struct {
	topics map[string]map[int][]api.Record
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{topics: make(map[string]map[int][]api.Record)}
}

func (b *fakeBroker) CreateTopic(ctx context.Context, name string, cfg api.TopicConfig) error {
	_ = ctx
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
	_ = group
	_ = topic
	_ = partition
	_ = offset
	return nil
}

func (b *fakeBroker) FetchCommitted(ctx context.Context, group string, topic string, partition int) (api.Offset, error) {
	_ = ctx
	_ = group
	_ = topic
	_ = partition
	return 0, nil
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
	ctReq := &CreateTopicRequest{Topic: "a", Partitions: 1, ReplicationFactor: 1}
	ctPayload, _ := encodeCreateTopicRequest(ctReq)
	respPayload, err := send(api.APIKeyCreateTopic, 1, ctPayload)
	if err != nil {
		t.Fatalf("create-topic send: %v", err)
	}
	ctResp, err := decodeCreateTopicResponse(respPayload)
	if err != nil || ctResp.Error != api.ErrNone {
		t.Fatalf("create-topic resp: %v err=%v", ctResp, err)
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
}
