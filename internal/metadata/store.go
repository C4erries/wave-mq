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

	"github.com/c4erries/wave-mq/pkg/api"
)

const (
	minRecordSize  = 4 + 2 // crc32c + at least version+type
	minPayloadSize = 2

	eventVersionV1 uint8 = 1

	eventTypeCreateTopic uint8 = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

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
	Partitions        []PartitionSpec
}

// TopicState represents recovered metadata for a topic.
type TopicState struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
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
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(b)-4))
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

	if len(ev.Partitions) != ev.NumPartitions {
		return nil, fmt.Errorf("partition specs must match num partitions")
	}

	buf := &bytes.Buffer{}
	buf.WriteByte(eventVersionV1)
	buf.WriteByte(eventTypeCreateTopic)

	if len(ev.Name) > 65535 {
		return nil, fmt.Errorf("topic name too long")
	}

	if err := binary.Write(buf, binary.LittleEndian, uint16(len(ev.Name))); err != nil {
		return nil, err
	}

	if _, err := buf.WriteString(ev.Name); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, uint16(ev.NumPartitions)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, uint16(ev.ReplicationFactor)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, uint16(len(ev.Partitions))); err != nil {
		return nil, err
	}

	for _, p := range ev.Partitions {
		if len(p.Replicas) == 0 {
			return nil, fmt.Errorf("partition %d has no replicas", p.ID)
		}

		if ev.ReplicationFactor > 0 && len(p.Replicas) != ev.ReplicationFactor {
			return nil, fmt.Errorf("partition %d replica count %d != rf %d", p.ID, len(p.Replicas), ev.ReplicationFactor)
		}

		if err := binary.Write(buf, binary.LittleEndian, int32(p.ID)); err != nil {
			return nil, err
		}

		if len(p.Replicas) > 65535 {
			return nil, fmt.Errorf("too many replicas for partition %d", p.ID)
		}

		if err := binary.Write(buf, binary.LittleEndian, uint16(len(p.Replicas))); err != nil {
			return nil, err
		}

		for _, r := range p.Replicas {
			if err := binary.Write(buf, binary.LittleEndian, r.BrokerID); err != nil {
				return nil, err
			}

			if err := buf.WriteByte(byte(r.Role)); err != nil {
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
	if version != eventVersionV1 {
		return ev, fmt.Errorf("unsupported create-topic version %d", version)
	}

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
		Partitions:        make([]PartitionSpec, 0, specCount),
	}
	for i := 0; i < int(specCount); i++ {
		var pid int32
		if err := binary.Read(reader, binary.LittleEndian, &pid); err != nil {
			return ev, err
		}

		var replicaCount uint16
		if err := binary.Read(reader, binary.LittleEndian, &replicaCount); err != nil {
			return ev, err
		}

		replicas := make([]ReplicaSpec, 0, replicaCount)
		for j := 0; j < int(replicaCount); j++ {
			var brokerID int32
			if err := binary.Read(reader, binary.LittleEndian, &brokerID); err != nil {
				return ev, err
			}

			roleByte, err := reader.ReadByte()
			if err != nil {
				return ev, err
			}

			var epoch int32
			if err := binary.Read(reader, binary.LittleEndian, &epoch); err != nil {
				return ev, err
			}

			replicas = append(replicas, ReplicaSpec{
				BrokerID:    brokerID,
				Role:        api.PartitionRole(roleByte),
				LeaderEpoch: epoch,
			})
		}

		ev.Partitions = append(ev.Partitions, PartitionSpec{
			ID:       pid,
			Replicas: replicas,
		})
	}

	if len(ev.Partitions) != ev.NumPartitions {
		return ev, fmt.Errorf("create-topic partition count mismatch: expected %d, got %d", ev.NumPartitions, len(ev.Partitions))
	}

	return ev, nil
}
