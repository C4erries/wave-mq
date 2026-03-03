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
	maxInt16        = int(^uint16(0) >> 1)
	maxInt32        = int(^uint32(0) >> 1)
	maxUint32       = uint64(^uint32(0))
	maxFrameLength  = uint32(16 << 20)
	maxItemCount    = int32(100_000)
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

	length, err := toUint32Checked(len(data)-4, "frame length")
	if err != nil {
		return nil, err
	}

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

	if length > maxFrameLength {
		return 0, 0, nil, fmt.Errorf("frame length %d exceeds max %d", length, maxFrameLength)
	}

	if uint64(length) > uint64(maxInt32) {
		return 0, 0, nil, fmt.Errorf("frame length %d exceeds int32 max", length)
	}

	apiKeyRaw, err := uint16ToInt16(binary.BigEndian.Uint16(header[4:6]), "api key")
	if err != nil {
		return 0, 0, nil, err
	}

	apiKey := api.APIKey(apiKeyRaw)

	version, err := uint16ToInt16(binary.BigEndian.Uint16(header[6:8]), "version")
	if err != nil {
		return 0, 0, nil, err
	}

	if version != currentVersion {
		return 0, 0, nil, fmt.Errorf("unsupported version %d", version)
	}

	corr, err := uint32ToInt32(binary.BigEndian.Uint32(header[8:12]), "correlation id")
	if err != nil {
		return 0, 0, nil, err
	}

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
	length, err := toInt16Checked(len(s), "string length")
	if err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}

	if s != "" {
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

	length, err := toInt32Checked(len(b), "bytes length")
	if err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, length); err != nil {
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
	count, err := toInt32Checked(len(headers), "headers count")
	if err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, count); err != nil {
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

	if n > maxItemCount {
		return nil, fmt.Errorf("header count %d exceeds max %d", n, maxItemCount)
	}

	headers := make([]api.Header, 0, int(n))
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
	var (
		res    api.Record
		offset int64
	)

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

	partitions, err := toInt32Checked(req.Partitions, "partitions")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partitions); err != nil {
		return nil, err
	}

	replicationFactor, err := toInt32Checked(req.ReplicationFactor, "replication factor")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, replicationFactor); err != nil {
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

	partition, err := toInt32Checked(req.Partition, "partition")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
		return nil, err
	}

	recordCount, err := toInt32Checked(len(req.Records), "record count")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, recordCount); err != nil {
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

		recordLen, err := toInt32Checked(len(recBytes), "record bytes length")
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, recordLen); err != nil {
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

	if n < 0 {
		return nil, fmt.Errorf("negative record count")
	}

	if n > maxItemCount {
		return nil, fmt.Errorf("record count %d exceeds max %d", n, maxItemCount)
	}

	recs := make([]api.Record, 0, int(n))
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

	partition, err := toInt32Checked(req.Partition, "partition")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
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

	recordCount, err := toInt32Checked(len(resp.Records), "record count")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, recordCount); err != nil {
		return nil, err
	}

	for _, r := range resp.Records {
		recBytes, err := encodeRecord(r)
		if err != nil {
			return nil, err
		}

		recordLen, err := toInt32Checked(len(recBytes), "record bytes length")
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, recordLen); err != nil {
			return nil, err
		}

		if _, err := buf.Write(recBytes); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(buf, binary.BigEndian, int64(resp.HighWatermark)); err != nil {
		return nil, err
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

	if n < 0 {
		return nil, fmt.Errorf("negative record count")
	}

	if n > maxItemCount {
		return nil, fmt.Errorf("record count %d exceeds max %d", n, maxItemCount)
	}

	recs := make([]api.Record, 0, int(n))
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

	var hwm int64
	if err := binary.Read(buf, binary.BigEndian, &hwm); err != nil {
		return nil, err
	}

	return &FetchResponse{Error: api.ErrorCode(ec), Records: recs, HighWatermark: api.Offset(hwm)}, nil
}

func encodeMetadataRequest(req *MetadataRequest) ([]byte, error) {
	buf := &bytes.Buffer{}

	topicCount, err := toInt32Checked(len(req.Topics), "topic count")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, topicCount); err != nil {
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

	if n < 0 {
		return nil, fmt.Errorf("negative topic count")
	}

	if n > maxItemCount {
		return nil, fmt.Errorf("topic count %d exceeds max %d", n, maxItemCount)
	}

	topics := make([]string, 0, int(n))
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

	partitionsCount, err := toInt32Checked(len(resp.Partitions), "partition count")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partitionsCount); err != nil {
		return nil, err
	}

	for _, p := range resp.Partitions {
		if err := putString(buf, p.Replica.Topic); err != nil {
			return nil, err
		}

		partition, err := toInt32Checked(p.Replica.Partition, "replica partition")
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
			return nil, err
		}

		brokerID, err := toInt32Checked(p.Replica.BrokerID, "replica broker id")
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, brokerID); err != nil {
			return nil, err
		}

		role, err := partitionRoleToInt16(p.Replica.Role)
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, role); err != nil {
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

		leader, err := toInt32Checked(p.Leader, "leader")
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, leader); err != nil {
			return nil, err
		}

		replicasCount, err := toInt32Checked(len(p.Replicas), "replicas count")
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, replicasCount); err != nil {
			return nil, err
		}

		for _, r := range p.Replicas {
			replicaID, err := toInt32Checked(r, "replica id")
			if err != nil {
				return nil, err
			}

			if err := binary.Write(buf, binary.BigEndian, replicaID); err != nil {
				return nil, err
			}
		}

		isrCount, err := toInt32Checked(len(p.ISR), "isr count")
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, isrCount); err != nil {
			return nil, err
		}

		for _, r := range p.ISR {
			isrID, err := toInt32Checked(r, "isr id")
			if err != nil {
				return nil, err
			}

			if err := binary.Write(buf, binary.BigEndian, isrID); err != nil {
				return nil, err
			}
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

	if n < 0 {
		return nil, fmt.Errorf("negative partition count")
	}

	if n > maxItemCount {
		return nil, fmt.Errorf("partition count %d exceeds max %d", n, maxItemCount)
	}

	parts := make([]api.PartitionMetadata, 0, int(n))
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

		var leader int32
		if err := binary.Read(buf, binary.BigEndian, &leader); err != nil {
			return nil, err
		}

		var replicasN int32
		if err := binary.Read(buf, binary.BigEndian, &replicasN); err != nil {
			return nil, err
		}

		if replicasN < 0 {
			return nil, fmt.Errorf("negative replicas count")
		}

		if replicasN > maxItemCount {
			return nil, fmt.Errorf("replicas count %d exceeds max %d", replicasN, maxItemCount)
		}

		replicas := make([]int, 0, int(replicasN))
		for i := int32(0); i < replicasN; i++ {
			var rid int32
			if err := binary.Read(buf, binary.BigEndian, &rid); err != nil {
				return nil, err
			}

			replicas = append(replicas, int(rid))
		}

		var isrN int32
		if err := binary.Read(buf, binary.BigEndian, &isrN); err != nil {
			return nil, err
		}

		if isrN < 0 {
			return nil, fmt.Errorf("negative isr count")
		}

		if isrN > maxItemCount {
			return nil, fmt.Errorf("isr count %d exceeds max %d", isrN, maxItemCount)
		}

		isr := make([]int, 0, int(isrN))
		for i := int32(0); i < isrN; i++ {
			var rid int32
			if err := binary.Read(buf, binary.BigEndian, &rid); err != nil {
				return nil, err
			}

			isr = append(isr, int(rid))
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
			Leader:        int(leader),
			Replicas:      replicas,
			ISR:           isr,
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

func encodeCommitOffsetRequest(req *CommitOffsetRequest) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := putString(buf, req.Group); err != nil {
		return nil, err
	}

	if err := putString(buf, req.Topic); err != nil {
		return nil, err
	}

	partition, err := toInt32Checked(req.Partition, "partition")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int64(req.Offset)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeCommitOffsetRequest(payload []byte) (*CommitOffsetRequest, error) {
	buf := bytes.NewBuffer(payload)

	group, err := readString(buf)
	if err != nil {
		return nil, err
	}

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

	return &CommitOffsetRequest{
		Group:     group,
		Topic:     topic,
		Partition: int(partition),
		Offset:    api.Offset(offset),
	}, nil
}

func encodeCommitOffsetResponse(resp *CommitOffsetResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeCommitOffsetResponse(payload []byte) (*CommitOffsetResponse, error) {
	buf := bytes.NewBuffer(payload)

	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}

	return &CommitOffsetResponse{Error: api.ErrorCode(ec)}, nil
}

func encodeFetchCommittedRequest(req *FetchCommittedRequest) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := putString(buf, req.Group); err != nil {
		return nil, err
	}

	if err := putString(buf, req.Topic); err != nil {
		return nil, err
	}

	partition, err := toInt32Checked(req.Partition, "partition")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeFetchCommittedRequest(payload []byte) (*FetchCommittedRequest, error) {
	buf := bytes.NewBuffer(payload)

	group, err := readString(buf)
	if err != nil {
		return nil, err
	}

	topic, err := readString(buf)
	if err != nil {
		return nil, err
	}

	var partition int32
	if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	return &FetchCommittedRequest{
		Group:     group,
		Topic:     topic,
		Partition: int(partition),
	}, nil
}

func encodeFetchCommittedResponse(resp *FetchCommittedResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int64(resp.Offset)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeFetchCommittedResponse(payload []byte) (*FetchCommittedResponse, error) {
	buf := bytes.NewBuffer(payload)

	var offset int64
	if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
		return nil, err
	}

	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}

	return &FetchCommittedResponse{
		Offset: api.Offset(offset),
		Error:  api.ErrorCode(ec),
	}, nil
}

func encodeListOffsetsRequest(req *ListOffsetsRequest) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := putString(buf, req.Topic); err != nil {
		return nil, err
	}

	partition, err := toInt32Checked(req.Partition, "partition")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeListOffsetsRequest(payload []byte) (*ListOffsetsRequest, error) {
	buf := bytes.NewBuffer(payload)

	topic, err := readString(buf)
	if err != nil {
		return nil, err
	}

	var partition int32
	if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	return &ListOffsetsRequest{
		Topic:     topic,
		Partition: int(partition),
	}, nil
}

func encodeListOffsetsResponse(resp *ListOffsetsResponse) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, int16(resp.Error)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int64(resp.Earliest)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int64(resp.Latest)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeListOffsetsResponse(payload []byte) (*ListOffsetsResponse, error) {
	buf := bytes.NewBuffer(payload)

	var ec int16
	if err := binary.Read(buf, binary.BigEndian, &ec); err != nil {
		return nil, err
	}

	var earliest int64
	if err := binary.Read(buf, binary.BigEndian, &earliest); err != nil {
		return nil, err
	}

	var latest int64
	if err := binary.Read(buf, binary.BigEndian, &latest); err != nil {
		return nil, err
	}

	return &ListOffsetsResponse{
		Error:    api.ErrorCode(ec),
		Earliest: api.Offset(earliest),
		Latest:   api.Offset(latest),
	}, nil
}

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

func EncodeCommitOffsetRequest(req *CommitOffsetRequest) ([]byte, error) {
	return encodeCommitOffsetRequest(req)
}

func DecodeCommitOffsetRequest(p []byte) (*CommitOffsetRequest, error) {
	return decodeCommitOffsetRequest(p)
}

func EncodeCommitOffsetResponse(resp *CommitOffsetResponse) ([]byte, error) {
	return encodeCommitOffsetResponse(resp)
}

func DecodeCommitOffsetResponse(p []byte) (*CommitOffsetResponse, error) {
	return decodeCommitOffsetResponse(p)
}

func EncodeFetchCommittedRequest(req *FetchCommittedRequest) ([]byte, error) {
	return encodeFetchCommittedRequest(req)
}

func DecodeFetchCommittedRequest(p []byte) (*FetchCommittedRequest, error) {
	return decodeFetchCommittedRequest(p)
}

func EncodeFetchCommittedResponse(resp *FetchCommittedResponse) ([]byte, error) {
	return encodeFetchCommittedResponse(resp)
}

func DecodeFetchCommittedResponse(p []byte) (*FetchCommittedResponse, error) {
	return decodeFetchCommittedResponse(p)
}

func EncodeListOffsetsRequest(req *ListOffsetsRequest) ([]byte, error) {
	return encodeListOffsetsRequest(req)
}

func DecodeListOffsetsResponse(p []byte) (*ListOffsetsResponse, error) {
	return decodeListOffsetsResponse(p)
}

func toUint32Checked(n int, field string) (uint32, error) {
	if n < 0 {
		return 0, fmt.Errorf("%s is negative: %d", field, n)
	}

	if uint64(n) > maxUint32 {
		return 0, fmt.Errorf("%s exceeds uint32 max: %d", field, n)
	}

	return uint32(n), nil // #nosec G115 -- bounds checked above.
}

func toInt16Checked(n int, field string) (int16, error) {
	if n < 0 || n > maxInt16 {
		return 0, fmt.Errorf("%s out of int16 range: %d", field, n)
	}

	return int16(n), nil // #nosec G115 -- bounds checked above.
}

func toInt32Checked(n int, field string) (int32, error) {
	if n < 0 || n > maxInt32 {
		return 0, fmt.Errorf("%s out of int32 range: %d", field, n)
	}

	return int32(n), nil // #nosec G115 -- bounds checked above.
}

func uint16ToInt16(v uint16, field string) (int16, error) {
	if v > uint16(maxInt16) {
		return 0, fmt.Errorf("%s out of int16 range: %d", field, v)
	}

	return int16(v), nil // #nosec G115 -- bounds checked above.
}

func uint32ToInt32(v uint32, field string) (int32, error) {
	if v > uint32(maxInt32) {
		return 0, fmt.Errorf("%s out of int32 range: %d", field, v)
	}

	return int32(v), nil // #nosec G115 -- bounds checked above.
}

func partitionRoleToInt16(role api.PartitionRole) (int16, error) {
	v := int(role)
	if v < 0 || v > maxInt16 {
		return 0, fmt.Errorf("partition role out of int16 range: %d", v)
	}

	return int16(v), nil // #nosec G115 -- bounds checked above.
}
