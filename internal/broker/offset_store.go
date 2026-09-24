package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

const (
	maxInt32  = int(^uint32(0) >> 1)
	maxUint16 = int(^uint16(0))
	maxUint32 = uint64(^uint32(0))
)

var renameOffsetsLogFile = os.Rename

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

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()

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

	info, err := s.f.Stat()
	if err != nil {
		return offsets, err
	}

	size := info.Size()

	var pos int64

	header := make([]byte, 4)

	for {
		select {
		case <-ctx.Done():
			return offsets, ctx.Err()
		default:
		}

		if pos+4 > size {
			if pos < size {
				if err := s.truncateTail(pos); err != nil {
					return offsets, err
				}
			}

			if err := s.seekWritePosition(pos); err != nil {
				return offsets, err
			}

			return offsets, nil
		}

		if _, err := s.f.ReadAt(header, pos); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if err := s.truncateTail(pos); err != nil {
					return offsets, err
				}

				return offsets, nil
			}

			return offsets, err
		}

		length := binary.LittleEndian.Uint32(header)
		if length > uint32(maxInt32) || pos+4+int64(length) > size {
			if err := s.truncateTail(pos); err != nil {
				return offsets, err
			}

			return offsets, nil
		}

		record := make([]byte, int(length))
		if _, err := s.f.ReadAt(record, pos+4); err != nil {
			if err := s.truncateTail(pos); err != nil {
				return offsets, err
			}

			return offsets, nil
		}

		if err := validateOffsetRecord(record); err != nil {
			if err := s.truncateTail(pos); err != nil {
				return offsets, err
			}

			return offsets, nil
		}

		group, topic, partition, off, err := decodeOffsetRecord(record)
		if err != nil {
			if err := s.truncateTail(pos); err != nil {
				return offsets, err
			}

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

	groupLen, err := toUint16Length(len(group), "group length")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, groupLen); err != nil {
		return nil, err
	}

	if _, err := buf.WriteString(group); err != nil {
		return nil, err
	}

	topicLen, err := toUint16Length(len(topic), "topic length")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, topicLen); err != nil {
		return nil, err
	}

	if _, err := buf.WriteString(topic); err != nil {
		return nil, err
	}

	partitionID, err := toInt32(partition, "partition")
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, partitionID); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, int64(offset)); err != nil {
		return nil, err
	}

	b := buf.Bytes()

	length, err := toUint32Length(len(b) - 4)
	if err != nil {
		return nil, err
	}

	binary.LittleEndian.PutUint32(b[0:4], length)
	crc := crc32.Checksum(b[8:], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(b[4:8], crc)

	return b, nil
}

func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f == nil {
		return nil
	}

	f := s.f
	s.f = nil

	return f.Close()
}

// Compact rewrites the offset log with a single record per group/topic/partition.
// It is intended as a maintenance operation and is not invoked automatically.
func (s *OffsetStore) Compact(ctx context.Context, offsets map[string]map[string]map[int]api.Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)

	tmp, err := os.CreateTemp(dir, "offsets-*.log")
	if err != nil {
		return err
	}

	tmpPath := tmp.Name()

	for group, topics := range offsets {
		for topic, parts := range topics {
			for partition, off := range parts {
				select {
				case <-ctx.Done():
					tmp.Close()

					removeTempFile(tmpPath)

					return ctx.Err()
				default:
				}

				rec, err := encodeOffsetRecord(group, topic, partition, off)
				if err != nil {
					tmp.Close()

					removeTempFile(tmpPath)

					return err
				}

				if _, err := tmp.Write(rec); err != nil {
					tmp.Close()

					removeTempFile(tmpPath)

					return err
				}
			}
		}
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()

		removeTempFile(tmpPath)

		return err
	}

	if err := tmp.Close(); err != nil {
		removeTempFile(tmpPath)
		return err
	}
	// Persist temp file directory entry where supported. Some platforms/filesystems
	// do not support directory fsync; keep this as best-effort.
	syncDirBestEffort(dir)
	// Swap files.
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			removeTempFile(tmpPath)
			return err
		}

		s.f = nil
	}

	if err := renameOffsetsLogFile(tmpPath, s.path); err != nil { // #nosec G703 -- temp/target paths are controlled local filesystem paths.
		removeTempFile(tmpPath)

		if reopenErr := s.reopenWriterLocked(); reopenErr != nil {
			return fmt.Errorf("rename compacted offsets log: %w (reopen writer failed: %v)", err, reopenErr)
		}

		return err
	}
	// Persist rename where supported.
	syncDirBestEffort(dir)

	return s.reopenWriterLocked()
}

func (s *OffsetStore) truncateTail(pos int64) error {
	if err := s.f.Truncate(pos); err != nil {
		return err
	}

	return s.seekWritePosition(pos)
}

func (s *OffsetStore) seekWritePosition(pos int64) error {
	_, err := s.f.Seek(pos, io.SeekStart)

	return err
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

func toUint16Length(n int, field string) (uint16, error) {
	if n < 0 || n > maxUint16 {
		return 0, fmt.Errorf("%s out of uint16 range: %d", field, n)
	}

	return uint16(n), nil // #nosec G115 -- bounds checked above
}

func toInt32(n int, field string) (int32, error) {
	if n < -maxInt32-1 || n > maxInt32 {
		return 0, fmt.Errorf("%s out of int32 range: %d", field, n)
	}

	return int32(n), nil // #nosec G115 -- bounds checked above
}

func removeTempFile(path string) {
	_ = os.Remove(path) // #nosec G703 -- path is created by os.CreateTemp in Compact.
}

func (s *OffsetStore) reopenWriterLocked() error {
	newFile, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}

	if _, err := newFile.Seek(0, io.SeekEnd); err != nil {
		_ = newFile.Close()
		return err
	}

	s.f = newFile

	return nil
}

func syncDirBestEffort(path string) {
	dir, err := os.Open(path)
	if err != nil {
		return
	}
	defer dir.Close()

	_ = dir.Sync()
}
