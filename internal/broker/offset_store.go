package broker

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

// Offset WAL format (offsets.log):
// uint32 length (bytes after this field)
// uint32 crc32c  (over bytes after crc32c)
// uint16 groupLen | group bytes
// uint16 topicLen | topic bytes
// int32  partition
// int64  offset

type OffsetStore struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func NewOffsetStore(dataDir string) (*OffsetStore, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("data dir required for offset store")
	}
	path := filepath.Join(dataDir, "offsets.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &OffsetStore{f: f, path: path}, nil
}

// Recover replays the WAL and returns offsets grouped by group/topic/partition.
// On malformed or partial tail, truncates to the last valid position.
func (s *OffsetStore) Recover(ctx context.Context) (map[string]map[string]map[int]api.Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offsets := make(map[string]map[string]map[int]api.Offset)
	var pos int64
	header := make([]byte, 4)
	for {
		select {
		case <-ctx.Done():
			return offsets, ctx.Err()
		default:
		}
		if _, err := s.f.ReadAt(header, pos); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return offsets, nil
			}
			return offsets, err
		}
		length := binary.LittleEndian.Uint32(header)
		record := make([]byte, length)
		if _, err := s.f.ReadAt(record, pos+4); err != nil {
			_ = s.f.Truncate(pos)
			return offsets, nil
		}
		if err := validateOffsetRecord(record); err != nil {
			_ = s.f.Truncate(pos)
			return offsets, nil
		}
		group, topic, partition, off, err := decodeOffsetRecord(record)
		if err != nil {
			_ = s.f.Truncate(pos)
			return offsets, nil
		}
		if _, ok := offsets[group]; !ok {
			offsets[group] = make(map[string]map[int]api.Offset)
		}
		if _, ok := offsets[group][topic]; !ok {
			offsets[group][topic] = make(map[int]api.Offset)
		}
		offsets[group][topic][partition] = off
		pos += int64(4 + length)
	}
}

func validateOffsetRecord(record []byte) error {
	if len(record) < 4+2+2+4+8 {
		return fmt.Errorf("record too small")
	}
	crc := binary.LittleEndian.Uint32(record[:4])
	calculated := crc32.Checksum(record[4:], crc32.MakeTable(crc32.Castagnoli))
	if crc != calculated {
		return fmt.Errorf("crc mismatch")
	}
	return nil
}

func decodeOffsetRecord(record []byte) (string, string, int, api.Offset, error) {
	buf := bytes.NewBuffer(record[4:]) // skip crc
	var gl uint16
	if err := binary.Read(buf, binary.LittleEndian, &gl); err != nil {
		return "", "", 0, 0, err
	}
	group := make([]byte, gl)
	if _, err := io.ReadFull(buf, group); err != nil {
		return "", "", 0, 0, err
	}
	var tl uint16
	if err := binary.Read(buf, binary.LittleEndian, &tl); err != nil {
		return "", "", 0, 0, err
	}
	topic := make([]byte, tl)
	if _, err := io.ReadFull(buf, topic); err != nil {
		return "", "", 0, 0, err
	}
	var partition int32
	if err := binary.Read(buf, binary.LittleEndian, &partition); err != nil {
		return "", "", 0, 0, err
	}
	var off int64
	if err := binary.Read(buf, binary.LittleEndian, &off); err != nil {
		return "", "", 0, 0, err
	}
	return string(group), string(topic), int(partition), api.Offset(off), nil
}

func (s *OffsetStore) AppendCommit(ctx context.Context, group, topic string, partition int, offset api.Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	rec, err := encodeOffsetRecord(group, topic, partition, offset)
	if err != nil {
		return err
	}
	if _, err := s.f.Write(rec); err != nil {
		return err
	}
	return s.f.Sync()
}

func encodeOffsetRecord(group, topic string, partition int, offset api.Offset) ([]byte, error) {
	buf := &bytes.Buffer{}
	// length placeholder
	if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil { // crc placeholder
		return nil, err
	}
	if len(group) > 65535 || len(topic) > 65535 {
		return nil, fmt.Errorf("string too long")
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(group))); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(group)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(topic))); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(topic)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(partition)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int64(offset)); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	length := uint32(len(b) - 4)
	binary.LittleEndian.PutUint32(b[0:4], length)
	crc := crc32.Checksum(b[8:], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(b[4:8], crc)
	return b, nil
}

func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}
