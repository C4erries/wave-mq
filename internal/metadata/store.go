package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

const (
	minRecordSize  = 4 + 2 // crc32c + at least version+type
	minPayloadSize = 2

	eventVersionV1 uint8 = 1
	eventVersionV2 uint8 = 2

	eventTypeCreateTopic uint8 = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	maxUint16 = int(^uint16(0))
	maxUint32 = uint64(^uint32(0))
	maxByte   = int(^byte(0))
)

type unknownEvent struct {
	typ uint8
}

// ReplicaSpec describes a single replica of a partition.
type ReplicaSpec struct {
	BrokerID    int32
	Role        api.PartitionRole
	LeaderEpoch int32
}

// PartitionSpec captures replica layout for a partition.
type PartitionSpec struct {
	ID       int32
	Replicas []ReplicaSpec
}

// CreateTopicEvent is the persisted command to create a topic.
type CreateTopicEvent struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
	RetentionBytes    int64
	RetentionTime     time.Duration
	Partitions        []PartitionSpec
}

// TopicState represents recovered metadata for a topic.
type TopicState struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
	RetentionBytes    int64
	RetentionTime     time.Duration
	Partitions        []PartitionSpec
}

// RecoveredTopics aggregates topic metadata rebuilt from metadata.log.
type RecoveredTopics struct {
	Topics map[string]TopicState
}

// Store appends and replays metadata events from metadata.log.
type Store struct {
	mu   sync.Mutex
	f    *os.File
	path string
	end  int64
}

// NewStore opens (or creates) metadata.log under the broker data directory.
func NewStore(cfg api.BrokerConfig) (*Store, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir required for metadata store")
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	path := filepath.Join(cfg.DataDir, "metadata.log")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &Store{f: f, path: path, end: info.Size()}, nil
}

// AppendCreateTopic durably records a CreateTopic event.
func (s *Store) AppendCreateTopic(ctx context.Context, ev CreateTopicEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	payload, err := encodeCreateTopicEvent(ev)
	if err != nil {
		return err
	}

	rec, err := wrapRecord(payload)
	if err != nil {
		return err
	}

	if _, err := s.f.Seek(s.end, io.SeekStart); err != nil {
		return err
	}

	if _, err := s.f.Write(rec); err != nil {
		return err
	}

	s.end += int64(len(rec))

	return s.f.Sync()
}

// RecoverTopics replays metadata.log and truncates any malformed tail.
func (s *Store) RecoverTopics(ctx context.Context) (RecoveredTopics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	topics := make(map[string]TopicState)

	var pos int64

	lenBuf := make([]byte, 4)

	for {
		select {
		case <-ctx.Done():
			s.end = pos
			_, _ = s.f.Seek(s.end, io.SeekStart)

			return RecoveredTopics{Topics: topics}, ctx.Err()
		default:
		}

		if _, err := s.f.ReadAt(lenBuf, pos); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				s.end = pos
				_, _ = s.f.Seek(s.end, io.SeekStart)

				return RecoveredTopics{Topics: topics}, err
			}

			_ = s.f.Truncate(pos)

			break
		}

		length := binary.LittleEndian.Uint32(lenBuf)
		if length == 0 {
			_ = s.f.Truncate(pos)
			break
		}

		record := make([]byte, length)
		if _, err := s.f.ReadAt(record, pos+4); err != nil {
			_ = s.f.Truncate(pos)
			break
		}

		ev, err := decodeRecord(record)
		if err != nil {
			_ = s.f.Truncate(pos)
			break
		}

		switch e := ev.(type) {
		case CreateTopicEvent:
			if _, exists := topics[e.Name]; exists {
				pos += int64(4 + length)
				continue
			}

			topics[e.Name] = TopicState(e)
		default:
			// Unknown events are ignored for forward compatibility.
		}

		pos += int64(4 + length)
	}

	s.end = pos
	if _, err := s.f.Seek(s.end, io.SeekStart); err != nil {
		return RecoveredTopics{Topics: topics}, err
	}

	return RecoveredTopics{Topics: topics}, nil
}

// Close releases the underlying file descriptor.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f != nil {
		return s.f.Close()
	}

	return nil
}

// wrapRecord adds length+crc32c prefix around the payload.
func wrapRecord(payload []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, err
	}

	if _, err := buf.Write(payload); err != nil {
		return nil, err
	}

	b := buf.Bytes()

	recordLen, err := toUint32Length(len(b) - 4)
	if err != nil {
		return nil, err
	}

	binary.LittleEndian.PutUint32(b[0:4], recordLen)
	crc := crc32.Checksum(b[8:], crcTable)
	binary.LittleEndian.PutUint32(b[4:8], crc)

	return b, nil
}

func decodeRecord(record []byte) (interface{}, error) {
	if len(record) < minRecordSize {
		return nil, fmt.Errorf("record too small")
	}

	crc := binary.LittleEndian.Uint32(record[:4])

	calculated := crc32.Checksum(record[4:], crcTable)
	if crc != calculated {
		return nil, fmt.Errorf("crc mismatch")
	}

	payload := record[4:]
	if len(payload) < minPayloadSize {
		return nil, fmt.Errorf("payload too small")
	}

	version := payload[0]
	typ := payload[1]
	body := payload[2:]

	switch typ {
	case eventTypeCreateTopic:
		return decodeCreateTopicEvent(version, body)
	default:
		return unknownEvent{typ: typ}, nil
	}
}

func encodeCreateTopicEvent(ev CreateTopicEvent) ([]byte, error) {
	if ev.Name == "" {
		return nil, fmt.Errorf("topic name required")
	}

	if ev.NumPartitions <= 0 {
		return nil, fmt.Errorf("partitions must be >0")
	}

	if ev.ReplicationFactor <= 0 {
		return nil, fmt.Errorf("replication factor must be >0")
	}

	if ev.RetentionBytes < -1 {
		return nil, fmt.Errorf("retention bytes must be >= -1")
	}

	if ev.RetentionTime < 0 {
		return nil, fmt.Errorf("retention time must be >= 0")
	}

	if len(ev.Partitions) != ev.NumPartitions {
		return nil, fmt.Errorf("partition specs must match num partitions")
	}

	buf := &bytes.Buffer{}
	buf.WriteByte(eventVersionV2)
	buf.WriteByte(eventTypeCreateTopic)

	if len(ev.Name) > 65535 {
		return nil, fmt.Errorf("topic name too long")
	}

	nameLen, err := toUint16("topic name length", len(ev.Name))
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, nameLen); err != nil {
		return nil, err
	}

	if _, err := buf.WriteString(ev.Name); err != nil {
		return nil, err
	}

	numPartitions, err := toUint16("num partitions", ev.NumPartitions)
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, numPartitions); err != nil {
		return nil, err
	}

	replicationFactor, err := toUint16("replication factor", ev.ReplicationFactor)
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, replicationFactor); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, ev.RetentionBytes); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, int64(ev.RetentionTime)); err != nil {
		return nil, err
	}

	partitionCount, err := toUint16("partition spec count", len(ev.Partitions))
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, partitionCount); err != nil {
		return nil, err
	}

	for _, p := range ev.Partitions {
		if len(p.Replicas) == 0 {
			return nil, fmt.Errorf("partition %d has no replicas", p.ID)
		}

		if ev.ReplicationFactor > 0 && len(p.Replicas) != ev.ReplicationFactor {
			return nil, fmt.Errorf("partition %d replica count %d != rf %d", p.ID, len(p.Replicas), ev.ReplicationFactor)
		}

		if err := binary.Write(buf, binary.LittleEndian, p.ID); err != nil {
			return nil, err
		}

		if len(p.Replicas) > 65535 {
			return nil, fmt.Errorf("too many replicas for partition %d", p.ID)
		}

		replicaCount, err := toUint16(fmt.Sprintf("partition %d replica count", p.ID), len(p.Replicas))
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.LittleEndian, replicaCount); err != nil {
			return nil, err
		}

		for _, r := range p.Replicas {
			if err := binary.Write(buf, binary.LittleEndian, r.BrokerID); err != nil {
				return nil, err
			}

			role, err := partitionRoleToByte(r.Role)
			if err != nil {
				return nil, err
			}

			if err := buf.WriteByte(role); err != nil {
				return nil, err
			}

			if err := binary.Write(buf, binary.LittleEndian, r.LeaderEpoch); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func decodeCreateTopicEvent(version uint8, data []byte) (CreateTopicEvent, error) {
	var ev CreateTopicEvent
	switch version {
	case eventVersionV1:
		return decodeCreateTopicEventV1(data)
	case eventVersionV2:
		return decodeCreateTopicEventV2(data)
	default:
		return ev, fmt.Errorf("unsupported create-topic version %d", version)
	}
}

func decodeCreateTopicEventV2(data []byte) (CreateTopicEvent, error) {
	var ev CreateTopicEvent

	reader := bytes.NewReader(data)

	var nameLen uint16
	if err := binary.Read(reader, binary.LittleEndian, &nameLen); err != nil {
		return ev, err
	}

	name := make([]byte, nameLen)
	if _, err := io.ReadFull(reader, name); err != nil {
		return ev, err
	}

	var parts uint16
	if err := binary.Read(reader, binary.LittleEndian, &parts); err != nil {
		return ev, err
	}

	var rf uint16
	if err := binary.Read(reader, binary.LittleEndian, &rf); err != nil {
		return ev, err
	}

	var retentionBytes int64
	if err := binary.Read(reader, binary.LittleEndian, &retentionBytes); err != nil {
		return ev, err
	}

	var retentionNanos int64
	if err := binary.Read(reader, binary.LittleEndian, &retentionNanos); err != nil {
		return ev, err
	}

	if retentionBytes < -1 {
		return ev, fmt.Errorf("invalid retention bytes %d", retentionBytes)
	}

	if retentionNanos < 0 {
		return ev, fmt.Errorf("invalid retention duration %d", retentionNanos)
	}

	var specCount uint16
	if err := binary.Read(reader, binary.LittleEndian, &specCount); err != nil {
		return ev, err
	}

	ev = CreateTopicEvent{
		Name:              string(name),
		NumPartitions:     int(parts),
		ReplicationFactor: int(rf),
		RetentionBytes:    retentionBytes,
		RetentionTime:     time.Duration(retentionNanos),
	}

	partitions, err := decodePartitionSpecs(reader, specCount)
	if err != nil {
		return ev, err
	}

	ev.Partitions = partitions

	if len(ev.Partitions) != ev.NumPartitions {
		return ev, fmt.Errorf("create-topic partition count mismatch: expected %d, got %d", ev.NumPartitions, len(ev.Partitions))
	}

	return ev, nil
}

func decodeCreateTopicEventV1(data []byte) (CreateTopicEvent, error) {
	var ev CreateTopicEvent

	reader := bytes.NewReader(data)

	var nameLen uint16
	if err := binary.Read(reader, binary.LittleEndian, &nameLen); err != nil {
		return ev, err
	}

	name := make([]byte, nameLen)
	if _, err := io.ReadFull(reader, name); err != nil {
		return ev, err
	}

	var parts uint16
	if err := binary.Read(reader, binary.LittleEndian, &parts); err != nil {
		return ev, err
	}

	var rf uint16
	if err := binary.Read(reader, binary.LittleEndian, &rf); err != nil {
		return ev, err
	}

	var specCount uint16
	if err := binary.Read(reader, binary.LittleEndian, &specCount); err != nil {
		return ev, err
	}

	ev = CreateTopicEvent{
		Name:              string(name),
		NumPartitions:     int(parts),
		ReplicationFactor: int(rf),
	}

	partitions, err := decodePartitionSpecs(reader, specCount)
	if err != nil {
		return ev, err
	}

	ev.Partitions = partitions

	if len(ev.Partitions) != ev.NumPartitions {
		return ev, fmt.Errorf("create-topic partition count mismatch: expected %d, got %d", ev.NumPartitions, len(ev.Partitions))
	}

	return ev, nil
}

func decodePartitionSpecs(reader *bytes.Reader, specCount uint16) ([]PartitionSpec, error) {
	partitions := make([]PartitionSpec, 0, specCount)
	for i := 0; i < int(specCount); i++ {
		var pid int32
		if err := binary.Read(reader, binary.LittleEndian, &pid); err != nil {
			return nil, err
		}

		var replicaCount uint16
		if err := binary.Read(reader, binary.LittleEndian, &replicaCount); err != nil {
			return nil, err
		}

		replicas, err := decodeReplicaSpecs(reader, replicaCount)
		if err != nil {
			return nil, err
		}

		partitions = append(partitions, PartitionSpec{
			ID:       pid,
			Replicas: replicas,
		})
	}

	return partitions, nil
}

func decodeReplicaSpecs(reader *bytes.Reader, replicaCount uint16) ([]ReplicaSpec, error) {
	replicas := make([]ReplicaSpec, 0, replicaCount)
	for j := 0; j < int(replicaCount); j++ {
		var brokerID int32
		if err := binary.Read(reader, binary.LittleEndian, &brokerID); err != nil {
			return nil, err
		}

		roleByte, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}

		var epoch int32
		if err := binary.Read(reader, binary.LittleEndian, &epoch); err != nil {
			return nil, err
		}

		replicas = append(replicas, ReplicaSpec{
			BrokerID:    brokerID,
			Role:        api.PartitionRole(roleByte),
			LeaderEpoch: epoch,
		})
	}

	return replicas, nil
}

func toUint32Length(n int) (uint32, error) {
	if n < 0 {
		return 0, fmt.Errorf("negative length %d", n)
	}

	if uint64(n) > maxUint32 {
		return 0, fmt.Errorf("length %d exceeds uint32 max", n)
	}

	return uint32(n), nil // #nosec G115 -- bounds checked above
}

func toUint16(field string, n int) (uint16, error) {
	if n < 0 || n > maxUint16 {
		return 0, fmt.Errorf("%s out of range: %d", field, n)
	}

	return uint16(n), nil // #nosec G115 -- bounds checked above
}

func partitionRoleToByte(role api.PartitionRole) (byte, error) {
	v := int(role)
	if v < 0 || v > maxByte {
		return 0, fmt.Errorf("partition role out of range: %d", v)
	}

	return byte(v), nil // #nosec G115 -- bounds checked above
}
