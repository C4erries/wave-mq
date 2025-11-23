package netproto

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

// frame header format:
// uint32 length (bytes following this field)
// int16  apiKey
// int16  version
// int32  correlationID
// int16  flags

const (
	frameHeaderSize = 4 + 2 + 2 + 4 + 2
	currentVersion  = int16(0)
)

func encodeRequestFrame(apiKey api.APIKey, correlationID int32, payload []byte) ([]byte, error) {
	return encodeFrame(apiKey, correlationID, payload)
}

func encodeResponseFrame(apiKey api.APIKey, correlationID int32, payload []byte) ([]byte, error) {
	return encodeFrame(apiKey, correlationID, payload)
}

func encodeFrame(apiKey api.APIKey, correlationID int32, payload []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	// length placeholder
	if err := binary.Write(buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int16(apiKey)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, currentVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, correlationID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
		return nil, err
	}
	if _, err := buf.Write(payload); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	length := uint32(len(data) - 4)
	binary.BigEndian.PutUint32(data[0:4], length)
	return data, nil
}

func decodeRequestFrame(r io.Reader) (api.APIKey, int32, []byte, error) {
	return decodeFrame(r)
}

func decodeResponseFrame(r io.Reader) (api.APIKey, int32, []byte, error) {
	return decodeFrame(r)
}

func decodeFrame(r io.Reader) (api.APIKey, int32, []byte, error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length < uint32(frameHeaderSize-4) {
		return 0, 0, nil, fmt.Errorf("invalid frame length %d", length)
	}
	apiKey := api.APIKey(int16(binary.BigEndian.Uint16(header[4:6])))
	version := int16(binary.BigEndian.Uint16(header[6:8]))
	if version != currentVersion {
		return 0, 0, nil, fmt.Errorf("unsupported version %d", version)
	}
	corr := int32(binary.BigEndian.Uint32(header[8:12]))
	payloadLen := int(length) - (frameHeaderSize - 4)
	if payloadLen < 0 {
		return 0, 0, nil, fmt.Errorf("invalid payload length")
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, 0, nil, err
	}
	return apiKey, corr, payload, nil
}

// primitive encoders/decoders

func putString(w io.Writer, s string) error {
	if err := binary.Write(w, binary.BigEndian, int16(len(s))); err != nil {
		return err
	}
	if len(s) > 0 {
		_, err := w.Write([]byte(s))
		return err
	}
	return nil
}

func readString(r io.Reader) (string, error) {
	var l int16
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return "", err
	}
	if l < 0 {
		return "", fmt.Errorf("invalid string length %d", l)
	}
	if l == 0 {
		return "", nil
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func putBytes(w io.Writer, b []byte) error {
	if b == nil {
		return binary.Write(w, binary.BigEndian, int32(-1))
	}
	if err := binary.Write(w, binary.BigEndian, int32(len(b))); err != nil {
		return err
	}
	if len(b) > 0 {
		_, err := w.Write(b)
		return err
	}
	return nil
}

func readBytes(r io.Reader) ([]byte, error) {
	var l int32
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}
	if l == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func putHeaders(w io.Writer, headers []api.Header) error {
	if err := binary.Write(w, binary.BigEndian, int32(len(headers))); err != nil {
		return err
	}
	for _, h := range headers {
		if err := putString(w, h.Key); err != nil {
			return err
		}
		if err := putBytes(w, h.Value); err != nil {
			return err
		}
	}
	return nil
}

func readHeaders(r io.Reader) ([]api.Header, error) {
	var n int32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("negative header count")
	}
	headers := make([]api.Header, 0, n)
	for i := int32(0); i < n; i++ {
		k, err := readString(r)
		if err != nil {
			return nil, err
		}
		v, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		headers = append(headers, api.Header{Key: k, Value: v})
	}
	return headers, nil
}

func encodeRecord(r api.Record) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, r.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	if err := putBytes(buf, r.Key); err != nil {
		return nil, err
	}
	if err := putBytes(buf, r.Value); err != nil {
		return nil, err
	}
	if err := putHeaders(buf, r.Headers); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRecord(r io.Reader) (api.Record, error) {
	var res api.Record
	var offset int64
	if err := binary.Read(r, binary.BigEndian, &offset); err != nil {
		return res, err
	}
	res.Offset = api.Offset(offset)
	var ts int64
	if err := binary.Read(r, binary.BigEndian, &ts); err != nil {
		return res, err
	}
	res.Timestamp = time.Unix(0, ts)
	key, err := readBytes(r)
	if err != nil {
		return res, err
	}
	val, err := readBytes(r)
	if err != nil {
		return res, err
	}
	headers, err := readHeaders(r)
	if err != nil {
		return res, err
	}
	res.Key = key
	res.Value = val
	res.Headers = headers
	return res, nil
}

// message encoders/decoders

func encodeCreateTopicRequest(req *CreateTopicRequest) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := putString(buf, req.Topic); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(req.Partitions)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(req.ReplicationFactor)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCreateTopicRequest(payload []byte) (*CreateTopicRequest, error) {
	buf := bytes.NewBuffer(payload)
	topic, err := readString(buf)
	if err != nil {
		return nil, err
	}
	var partitions int32
	if err := binary.Read(buf, binary.BigEndian, &partitions); err != nil {
		return nil, err
	}
	var rf int32
	if err := binary.Read(buf, binary.BigEndian, &rf); err != nil {
		return nil, err
	}
	return &CreateTopicRequest{
		Topic:             topic,
		Partitions:        int(partitions),
		ReplicationFactor: int(rf),
	}, nil
}

func encodeCreateTopicResponse(resp *CreateTopicResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCreateTopicResponse(payload []byte) (*CreateTopicResponse, error) {
	buf := bytes.NewBuffer(payload)
	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}
	return &CreateTopicResponse{Error: api.ErrorCode(ec)}, nil
}

func encodeProduceRequest(req *ProduceRequest) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := putString(buf, req.Topic); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(req.Partition)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(len(req.Records))); err != nil {
		return nil, err
	}
	for _, r := range req.Records {
		if r.Timestamp.IsZero() {
			r.Timestamp = time.Now()
		}
		recBytes, err := encodeRecord(r)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int32(len(recBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(recBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeProduceRequest(payload []byte) (*ProduceRequest, error) {
	buf := bytes.NewBuffer(payload)
	topic, err := readString(buf)
	if err != nil {
		return nil, err
	}
	var partition int32
	if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
		return nil, err
	}
	var n int32
	if err := binary.Read(buf, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	recs := make([]api.Record, 0, n)
	for i := int32(0); i < n; i++ {
		var l int32
		if err := binary.Read(buf, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		if l < 0 || int(l) > buf.Len() {
			return nil, fmt.Errorf("invalid record length")
		}
		rBytes := buf.Next(int(l))
		rbuf := bytes.NewBuffer(rBytes)
		rec, err := decodeRecord(rbuf)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return &ProduceRequest{
		Topic:     topic,
		Partition: int(partition),
		Records:   recs,
	}, nil
}

func encodeProduceResponse(resp *ProduceResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int64(resp.BaseOffset)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeProduceResponse(payload []byte) (*ProduceResponse, error) {
	buf := bytes.NewBuffer(payload)
	var base int64
	if err := binary.Read(buf, binary.BigEndian, &base); err != nil {
		return nil, err
	}
	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}
	return &ProduceResponse{BaseOffset: api.Offset(base), Error: api.ErrorCode(ec)}, nil
}

func encodeFetchRequest(req *FetchRequest) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := putString(buf, req.Topic); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(req.Partition)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int64(req.Offset)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, req.MaxBytes); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeFetchRequest(payload []byte) (*FetchRequest, error) {
	buf := bytes.NewBuffer(payload)
	topic, err := readString(buf)
	if err != nil {
		return nil, err
	}
	var partition int32
	if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
		return nil, err
	}
	var offset int64
	if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
		return nil, err
	}
	var maxBytes int32
	if err := binary.Read(buf, binary.BigEndian, &maxBytes); err != nil {
		return nil, err
	}
	return &FetchRequest{
		Topic:     topic,
		Partition: int(partition),
		Offset:    api.Offset(offset),
		MaxBytes:  maxBytes,
	}, nil
}

func encodeFetchResponse(resp *FetchResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(len(resp.Records))); err != nil {
		return nil, err
	}
	for _, r := range resp.Records {
		recBytes, err := encodeRecord(r)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int32(len(recBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(recBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeFetchResponse(payload []byte) (*FetchResponse, error) {
	buf := bytes.NewBuffer(payload)
	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}
	var n int32
	if err := binary.Read(buf, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	recs := make([]api.Record, 0, n)
	for i := int32(0); i < n; i++ {
		var l int32
		if err := binary.Read(buf, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		rBytes := buf.Next(int(l))
		rbuf := bytes.NewBuffer(rBytes)
		rec, err := decodeRecord(rbuf)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return &FetchResponse{Error: api.ErrorCode(ec), Records: recs}, nil
}

func encodeMetadataRequest(req *MetadataRequest) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int32(len(req.Topics))); err != nil {
		return nil, err
	}
	for _, t := range req.Topics {
		if err := putString(buf, t); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeMetadataRequest(payload []byte) (*MetadataRequest, error) {
	buf := bytes.NewBuffer(payload)
	var n int32
	if err := binary.Read(buf, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	topics := make([]string, 0, n)
	for i := int32(0); i < n; i++ {
		s, err := readString(buf)
		if err != nil {
			return nil, err
		}
		topics = append(topics, s)
	}
	return &MetadataRequest{Topics: topics}, nil
}

func encodeMetadataResponse(resp *MetadataResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(len(resp.Partitions))); err != nil {
		return nil, err
	}
	for _, p := range resp.Partitions {
		if err := putString(buf, p.Replica.Topic); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int32(p.Replica.Partition)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int32(p.Replica.BrokerID)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int16(p.Replica.Role)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, p.Replica.LeaderEpoch); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int64(p.StartOffset)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int64(p.HighWatermark)); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeMetadataResponse(payload []byte) (*MetadataResponse, error) {
	buf := bytes.NewBuffer(payload)
	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}
	var n int32
	if err := binary.Read(buf, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	parts := make([]api.PartitionMetadata, 0, n)
	for i := int32(0); i < n; i++ {
		topic, err := readString(buf)
		if err != nil {
			return nil, err
		}
		var partition int32
		if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
			return nil, err
		}
		var brokerID int32
		if err := binary.Read(buf, binary.BigEndian, &brokerID); err != nil {
			return nil, err
		}
		var role int16
		if err := binary.Read(buf, binary.BigEndian, &role); err != nil {
			return nil, err
		}
		var epoch int32
		if err := binary.Read(buf, binary.BigEndian, &epoch); err != nil {
			return nil, err
		}
		var start int64
		if err := binary.Read(buf, binary.BigEndian, &start); err != nil {
			return nil, err
		}
		var hwm int64
		if err := binary.Read(buf, binary.BigEndian, &hwm); err != nil {
			return nil, err
		}
		parts = append(parts, api.PartitionMetadata{
			Replica: api.PartitionReplica{
				Topic:       topic,
				Partition:   int(partition),
				BrokerID:    int(brokerID),
				Role:        api.PartitionRole(role),
				LeaderEpoch: epoch,
			},
			StartOffset:   api.Offset(start),
			HighWatermark: api.Offset(hwm),
		})
	}
	return &MetadataResponse{
		Error:      api.ErrorCode(ec),
		Partitions: parts,
	}, nil
}

func encodePingRequest(_ *PingRequest) ([]byte, error) { return []byte{}, nil }
func decodePingRequest(_ []byte) (*PingRequest, error) { return &PingRequest{}, nil }
func encodePingResponse(resp *PingResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func decodePingResponse(payload []byte) (*PingResponse, error) {
	buf := bytes.NewBuffer(payload)
	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}
	return &PingResponse{Error: api.ErrorCode(ec)}, nil
}

// TODO: commit offset and fetch committed codecs when the broker supports them on the wire.

// Exported helpers for external packages (mbctl).
func EncodeRequestFrame(apiKey api.APIKey, corr int32, payload []byte) ([]byte, error) {
	return encodeRequestFrame(apiKey, corr, payload)
}

func DecodeResponseFrame(r io.Reader) (api.APIKey, int32, []byte, error) {
	return decodeResponseFrame(r)
}

func EncodeCreateTopicRequest(req *CreateTopicRequest) ([]byte, error) {
	return encodeCreateTopicRequest(req)
}
func EncodeCreateTopicResponse(resp *CreateTopicResponse) ([]byte, error) {
	return encodeCreateTopicResponse(resp)
}
func DecodeCreateTopicResponse(p []byte) (*CreateTopicResponse, error) {
	return decodeCreateTopicResponse(p)
}

func EncodeProduceRequest(req *ProduceRequest) ([]byte, error)    { return encodeProduceRequest(req) }
func EncodeProduceResponse(resp *ProduceResponse) ([]byte, error) { return encodeProduceResponse(resp) }
func DecodeProduceResponse(p []byte) (*ProduceResponse, error)    { return decodeProduceResponse(p) }

func EncodeFetchRequest(req *FetchRequest) ([]byte, error)    { return encodeFetchRequest(req) }
func EncodeFetchResponse(resp *FetchResponse) ([]byte, error) { return encodeFetchResponse(resp) }
func DecodeFetchResponse(p []byte) (*FetchResponse, error)    { return decodeFetchResponse(p) }

func EncodeMetadataRequest(req *MetadataRequest) ([]byte, error) { return encodeMetadataRequest(req) }
func DecodeMetadataResponse(p []byte) (*MetadataResponse, error) { return decodeMetadataResponse(p) }

func EncodePingRequest(req *PingRequest) ([]byte, error)    { return encodePingRequest(req) }
func EncodePingResponse(resp *PingResponse) ([]byte, error) { return encodePingResponse(resp) }
func DecodePingResponse(p []byte) (*PingResponse, error)    { return decodePingResponse(p) }
