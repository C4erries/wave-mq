package netproto

import (
	"bytes"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestFrameEncodeDecode(t *testing.T) {
	payload := []byte("hello")
	frame, err := encodeRequestFrame(api.APIKeyProduce, 42, payload)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	apiKey, corr, p, err := decodeRequestFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if apiKey != api.APIKeyProduce || corr != 42 {
		t.Fatalf("unexpected header: api=%d corr=%d", apiKey, corr)
	}
	if string(p) != "hello" {
		t.Fatalf("payload mismatch: %s", string(p))
	}
}

func TestCreateTopicCodec(t *testing.T) {
	req := &CreateTopicRequest{Topic: "a", Partitions: 3, ReplicationFactor: 1}
	payload, err := encodeCreateTopicRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeCreateTopicRequest(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Topic != req.Topic || got.Partitions != req.Partitions || got.ReplicationFactor != req.ReplicationFactor {
		t.Fatalf("roundtrip mismatch: %#v", got)
	}
	resp := &CreateTopicResponse{Error: api.ErrNone}
	rp, err := encodeCreateTopicResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodeCreateTopicResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.Error != resp.Error {
		t.Fatalf("resp mismatch")
	}
}

func TestProduceCodec(t *testing.T) {
	req := &ProduceRequest{
		Topic:     "t",
		Partition: 1,
		Records: []api.Record{
			{Offset: 0, Timestamp: time.Unix(1, 0), Key: []byte("k"), Value: []byte("v")},
		},
	}
	payload, err := encodeProduceRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeProduceRequest(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Topic != req.Topic || got.Partition != req.Partition || len(got.Records) != 1 || string(got.Records[0].Value) != "v" {
		t.Fatalf("roundtrip mismatch: %#v", got)
	}
	resp := &ProduceResponse{BaseOffset: 5, Error: api.ErrNone}
	rp, err := encodeProduceResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodeProduceResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.BaseOffset != resp.BaseOffset || dr.Error != resp.Error {
		t.Fatalf("resp mismatch")
	}
}

func TestFetchCodec(t *testing.T) {
	req := &FetchRequest{Topic: "t", Partition: 0, Offset: 2, MaxBytes: 100}
	payload, err := encodeFetchRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeFetchRequest(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Topic != req.Topic || got.Offset != req.Offset {
		t.Fatalf("roundtrip mismatch")
	}
	resp := &FetchResponse{
		Error: api.ErrNone,
		Records: []api.Record{
			{Offset: 2, Value: []byte("x")},
		},
		HighWatermark: 3,
	}
	rp, err := encodeFetchResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodeFetchResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.Error != resp.Error || len(dr.Records) != 1 || string(dr.Records[0].Value) != "x" || dr.HighWatermark != 3 {
		t.Fatalf("resp mismatch: %#v", dr)
	}
}

func TestMetadataCodec(t *testing.T) {
	req := &MetadataRequest{Topics: []string{"a", "b"}}
	payload, err := encodeMetadataRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeMetadataRequest(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Topics) != 2 || got.Topics[0] != "a" {
		t.Fatalf("roundtrip mismatch")
	}
	resp := &MetadataResponse{
		Error: api.ErrNone,
		Partitions: []api.PartitionMetadata{
			{
				Replica: api.PartitionReplica{
					Topic:     "a",
					Partition: 0,
					BrokerID:  1,
					Role:      api.RoleLeader,
				},
				StartOffset:   0,
				HighWatermark: 5,
			},
		},
	}
	rp, err := encodeMetadataResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodeMetadataResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.Error != resp.Error || len(dr.Partitions) != 1 || dr.Partitions[0].Replica.Topic != "a" {
		t.Fatalf("resp mismatch: %#v", dr)
	}
}

func TestPingCodec(t *testing.T) {
	req := &PingRequest{}
	p, err := encodePingRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(p) != 0 {
		t.Fatalf("expected empty payload")
	}
	if _, err := decodePingRequest(p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp := &PingResponse{Error: api.ErrNone}
	rp, err := encodePingResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodePingResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.Error != resp.Error {
		t.Fatalf("resp mismatch")
	}
}

func TestCommitOffsetCodec(t *testing.T) {
	req := &CommitOffsetRequest{Group: "g", Topic: "t", Partition: 1, Offset: 10}
	p, err := encodeCommitOffsetRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeCommitOffsetRequest(p)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Group != req.Group || got.Offset != req.Offset {
		t.Fatalf("roundtrip mismatch: %#v", got)
	}
	resp := &CommitOffsetResponse{Error: api.ErrNone}
	rp, err := encodeCommitOffsetResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodeCommitOffsetResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.Error != resp.Error {
		t.Fatalf("resp mismatch")
	}
}

func TestFetchCommittedCodec(t *testing.T) {
	req := &FetchCommittedRequest{Group: "g", Topic: "t", Partition: 0}
	p, err := encodeFetchCommittedRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeFetchCommittedRequest(p)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Group != req.Group || got.Partition != req.Partition {
		t.Fatalf("roundtrip mismatch")
	}
	resp := &FetchCommittedResponse{Offset: 12, Error: api.ErrNone}
	rp, err := encodeFetchCommittedResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodeFetchCommittedResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.Offset != resp.Offset || dr.Error != resp.Error {
		t.Fatalf("resp mismatch")
	}
}

func TestListOffsetsCodec(t *testing.T) {
	req := &ListOffsetsRequest{Topic: "t", Partition: 2}
	p, err := encodeListOffsetsRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeListOffsetsRequest(p)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Topic != req.Topic || got.Partition != req.Partition {
		t.Fatalf("roundtrip mismatch")
	}
	resp := &ListOffsetsResponse{Earliest: 1, Latest: 10, Error: api.ErrNone}
	rp, err := encodeListOffsetsResponse(resp)
	if err != nil {
		t.Fatalf("encode resp: %v", err)
	}
	dr, err := decodeListOffsetsResponse(rp)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if dr.Earliest != resp.Earliest || dr.Latest != resp.Latest || dr.Error != resp.Error {
		t.Fatalf("resp mismatch")
	}
}
