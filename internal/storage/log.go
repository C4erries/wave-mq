package storage

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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

// On-disk layout (simple and direct, ready for extension):
// - Directory per topic/partition: <DataDir>/<topic>/<partition>/
// - Segment files are named <baseOffset>.log (zero-padded 20 digits) alongside optional <baseOffset>.idx.
// - Record encoding inside *.log:
//     uint32 recordSize (bytes following this field)
//     uint32 crc32c     (over bytes following crc field)
//     int64  offset
//     int64  timestampUnixNano
//     int32  keyLen (>= -1; -1 means nil)
//     int32  valueLen (>= -1; -1 means nil)
//     int32  headersCount (>=0)
//     repeated headers:
//         int32 keyLen, bytes
//         int32 valLen, bytes
//     key bytes (if keyLen >= 0), value bytes (if valueLen >= 0)
// - Recovery invariant: offsets are strictly increasing starting at segment baseOffset;
//   scanning stops on malformed length/CRC/offset and the tail is truncated to the last valid record.
// - Sparse index per segment (<baseOffset>.idx):
//     int64 baseOffset
//     repeated entries:
//         int64 relativeOffset (offset - baseOffset)
//         int64 position (byte position of the record in .log, starting at length prefix)
//   IndexInterval controls how often entries are added (every N records).

const (
	logExt            = ".log"
	indexExt          = ".idx"
	defaultMaxSegment = int64(64 << 20) // 64MB
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type indexEntry struct {
	RelativeOffset int64
	Position       int64
}

// Config controls global storage behaviour such as segment sizing and retention.
type Config struct {
	DataDir         string
	MaxSegmentBytes int64
	IndexInterval   int
	SegmentMaxAge   time.Duration
	SyncOnAppend    bool
	MaxLogBytes     int64 // per-partition total size limit (-1 disables)
}

// LogOptions configures a single partition log instance.
type LogOptions struct {
	BaseOffset api.Offset
	Topic      string
	Partition  int
}

// Log represents an append-only segmented commit log for a single partition.
type Log interface {
	Append(ctx context.Context, record api.Record) (api.Offset, error)
	AppendBatch(ctx context.Context, records []api.Record) (api.Offset, error)
	Read(ctx context.Context, offset api.Offset, maxBytes int32) ([]api.Record, error)
	Truncate(ctx context.Context, offset api.Offset) error
	HighWatermark() api.Offset
	StartOffset() api.Offset
	Close() error
}

// Manager owns log instances across topics and partitions.
type Manager struct {
	cfg  Config
	mu   sync.Mutex
	logs map[string]*segmentedLog
}

// NewManager prepares storage manager state without touching disk yet.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir required")
	}
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = defaultMaxSegment
	}
	if cfg.IndexInterval == 0 {
		cfg.IndexInterval = 1024
	}
	if cfg.MaxLogBytes == 0 {
		cfg.MaxLogBytes = -1
	}
	return &Manager{cfg: cfg, logs: make(map[string]*segmentedLog)}, nil
}

// OpenLog opens or creates a segmented log for the given topic/partition.
func (m *Manager) OpenLog(opts LogOptions) (Log, error) {
	if opts.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	dir := filepath.Join(m.cfg.DataDir, opts.Topic, strconv.Itoa(opts.Partition))
	key := dir
	m.mu.Lock()
	if l, ok := m.logs[key]; ok {
		if !l.closed {
			m.mu.Unlock()
			return l, nil
		}
		delete(m.logs, key)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	l := &segmentedLog{
		opts: opts,
		cfg:  m.cfg,
		dir:  dir,
	}
	if err := l.bootstrap(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.logs[key] = l
	m.mu.Unlock()
	return l, nil
}

// Recover scans on-disk logs and repairs truncated tails if needed.
func (m *Manager) Recover(ctx context.Context) error {
	if _, err := os.Stat(m.cfg.DataDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return filepath.Walk(m.cfg.DataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), logExt) {
			return nil
		}
		dir := filepath.Dir(path)
		baseOffset, err := parseBaseOffset(info.Name())
		if err != nil {
			return nil
		}
		l := &segmentedLog{
			opts: LogOptions{BaseOffset: baseOffset},
			cfg:  m.cfg,
			dir:  dir,
		}
		return l.recoverSegmentFile(path, baseOffset)
	})
}

// Close flushes any open logs owned by the manager.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for k, l := range m.logs {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.logs, k)
	}
	return firstErr
}

type segment struct {
	baseOffset api.Offset
	nextOffset api.Offset
	file       *os.File
	size       int64
	path       string
	createdAt  time.Time

	idxPath string
	idxFile *os.File
	idx     []indexEntry
}

// segmentedLog is a segmented WAL for a single partition.
type segmentedLog struct {
	opts LogOptions
	cfg  Config
	dir  string

	mu          sync.RWMutex
	segments    []*segment
	nextOffset  api.Offset
	startOffset api.Offset
	closed      bool
}

func (l *segmentedLog) bootstrap() error {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	var segmentFiles []string
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), logExt) {
			continue
		}
		segmentFiles = append(segmentFiles, f.Name())
	}
	sort.Strings(segmentFiles)
	if len(segmentFiles) == 0 {
		return l.createSegment(l.opts.BaseOffset)
	}
	for _, name := range segmentFiles {
		base, err := parseBaseOffset(name)
		if err != nil {
			continue
		}
		path := filepath.Join(l.dir, name)
		seg, err := l.openSegment(path, base, true)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
		l.nextOffset = seg.nextOffset
		if l.startOffset == 0 || base < l.startOffset {
			l.startOffset = base
		}
	}
	// Always ensure an active writable segment exists.
	if len(l.segments) == 0 {
		return l.createSegment(l.opts.BaseOffset)
	}
	active := l.segments[len(l.segments)-1]
	if active.size >= l.cfg.MaxSegmentBytes && active.nextOffset > active.baseOffset {
		return l.createSegment(l.nextOffset)
	}
	return nil
}

func parseBaseOffset(name string) (api.Offset, error) {
	baseStr := strings.TrimSuffix(name, logExt)
	val, err := strconv.ParseInt(baseStr, 10, 64)
	return api.Offset(val), err
}

func (l *segmentedLog) createSegment(base api.Offset) error {
	path := filepath.Join(l.dir, fmt.Sprintf("%020d%s", base, logExt))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	idxPath := filepath.Join(l.dir, fmt.Sprintf("%020d%s", base, indexExt))
	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		_ = f.Close()
		return err
	}
	if err := binary.Write(idxFile, binary.LittleEndian, int64(base)); err != nil {
		_ = f.Close()
		_ = idxFile.Close()
		return err
	}
	seg := &segment{
		baseOffset: base,
		nextOffset: base,
		file:       f,
		size:       0,
		path:       path,
		createdAt:  time.Now(),
		idxPath:    idxPath,
		idxFile:    idxFile,
		idx:        make([]indexEntry, 0),
	}
	l.segments = append(l.segments, seg)
	if l.startOffset == 0 || base < l.startOffset {
		l.startOffset = base
	}
	l.nextOffset = base
	return nil
}

func (l *segmentedLog) recoverSegmentFile(path string, base api.Offset) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, goodBytes, err := scanSegment(f, base)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if goodBytes < info.Size() {
		if err := f.Truncate(goodBytes); err != nil {
			return err
		}
	}
	idxPath := strings.TrimSuffix(path, logExt) + indexExt
	_ = rebuildIndex(path, idxPath, base, l.cfg.IndexInterval)
	return nil
}

func (l *segmentedLog) openSegment(path string, base api.Offset, repair bool) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	nextOffset, goodBytes, err := scanSegment(f, base)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if repair {
		if stat, statErr := f.Stat(); statErr == nil && goodBytes < stat.Size() {
			if err := f.Truncate(goodBytes); err != nil {
				_ = f.Close()
				return nil, err
			}
		}
	}
	info, _ := f.Stat()
	seg := &segment{
		baseOffset: base,
		nextOffset: nextOffset,
		file:       f,
		size:       goodBytes,
		path:       path,
		createdAt:  info.ModTime(),
		idxPath:    strings.TrimSuffix(path, logExt) + indexExt,
	}
	if err := l.loadOrRebuildIndex(seg); err != nil {
		_ = f.Close()
		return nil, err
	}
	return seg, nil
}

// Append writes a single record to the WAL.
func (l *segmentedLog) Append(ctx context.Context, record api.Record) (api.Offset, error) {
	return l.AppendBatch(ctx, []api.Record{record})
}

// AppendBatch writes multiple records atomically to preserve ordering.
func (l *segmentedLog) AppendBatch(ctx context.Context, records []api.Record) (api.Offset, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return -1, fmt.Errorf("log closed")
	}
	if len(records) == 0 {
		return l.nextOffset, nil
	}
	active := l.ensureActiveSegmentLocked()
	baseOffset := l.nextOffset
	for i := range records {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		default:
		}
		records[i].Offset = l.nextOffset
		if records[i].Timestamp.IsZero() {
			records[i].Timestamp = time.Now()
		}
		b, err := encodeRecord(records[i])
		if err != nil {
			return -1, err
		}
		// Rotate if this record would exceed segment size and the segment already has data.
		if active.size+int64(len(b)) > l.cfg.MaxSegmentBytes && active.size > 0 {
			if l.cfg.SyncOnAppend {
				if err := active.file.Sync(); err != nil {
					return -1, err
				}
			}
			active = nil
			if err := l.createSegment(l.nextOffset); err != nil {
				return -1, err
			}
			active = l.segments[len(l.segments)-1]
		}
		pos := active.size
		if _, err := active.file.Write(b); err != nil {
			return -1, err
		}
		recordCount := int(active.nextOffset - active.baseOffset)
		if recordCount%l.cfg.IndexInterval == 0 && active.idxFile != nil {
			rel := int64(records[i].Offset - active.baseOffset)
			if err := binary.Write(active.idxFile, binary.LittleEndian, rel); err == nil {
				_ = binary.Write(active.idxFile, binary.LittleEndian, pos)
				active.idx = append(active.idx, indexEntry{RelativeOffset: rel, Position: pos})
			}
		}
		active.size += int64(len(b))
		l.nextOffset++
	}
	active.nextOffset = l.nextOffset
	if l.cfg.SyncOnAppend {
		if err := active.file.Sync(); err != nil {
			return -1, err
		}
	}
	l.enforceRetentionLocked()
	return baseOffset, nil
}

// Read returns records starting from the given offset up to maxBytes.
func (l *segmentedLog) Read(ctx context.Context, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, fmt.Errorf("log closed")
	}
	if offset >= l.nextOffset {
		return []api.Record{}, nil
	}
	var res []api.Record
	bytesRead := int32(0)
	for _, seg := range l.segments {
		if offset >= seg.nextOffset {
			continue
		}
		if offset < seg.baseOffset {
			offset = seg.baseOffset
		}
		records, err := readFromSegment(ctx, seg, offset, maxBytes-bytesRead)
		if err != nil {
			return nil, err
		}
		for _, r := range records {
			res = append(res, r)
			if maxBytes > 0 {
				bytesRead += int32(len(r.Value))
				if bytesRead >= maxBytes {
					return res, nil
				}
			}
		}
		offset = seg.nextOffset
	}
	return res, nil
}

// Truncate discards log data starting from offset (used for recovery).
func (l *segmentedLog) Truncate(ctx context.Context, offset api.Offset) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return fmt.Errorf("log closed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	var kept []*segment
	for i, seg := range l.segments {
		if offset <= seg.baseOffset {
			_ = seg.file.Truncate(0)
			_ = seg.file.Close()
			if seg.idxFile != nil {
				_ = seg.idxFile.Close()
			}
			continue
		}
		if offset >= seg.nextOffset {
			kept = append(kept, seg)
			continue
		}
		// Truncate inside this segment.
		pos, err := seekToOffset(seg, offset)
		if err != nil {
			return err
		}
		if err := seg.file.Truncate(pos); err != nil {
			return err
		}
		seg.size = pos
		seg.nextOffset = offset
		if seg.idxFile != nil {
			_ = seg.idxFile.Close()
		}
		_ = rebuildIndex(seg.path, seg.idxPath, seg.baseOffset, l.cfg.IndexInterval)
		seg.idxFile, seg.idx, _ = loadIndex(seg.idxPath, seg.baseOffset)
		kept = append(kept, seg)
		// Close and discard any following segments.
		for j := i + 1; j < len(l.segments); j++ {
			_ = l.segments[j].file.Close()
			if l.segments[j].idxFile != nil {
				_ = l.segments[j].idxFile.Close()
			}
		}
		break
	}
	l.segments = kept
	l.nextOffset = offset
	if len(l.segments) == 0 {
		if err := l.createSegment(offset); err != nil {
			return err
		}
	}
	if len(l.segments) > 0 {
		l.startOffset = l.segments[0].baseOffset
	}
	return nil
}

// HighWatermark exposes the durable offset boundary for the partition.
func (l *segmentedLog) HighWatermark() api.Offset {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.nextOffset == 0 {
		return -1
	}
	return l.nextOffset - 1
}

// StartOffset returns the earliest available offset in the log (after retention/truncation).
func (l *segmentedLog) StartOffset() api.Offset {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.startOffset
}

// Close flushes and releases file handles for the log.
func (l *segmentedLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var firstErr error
	for _, seg := range l.segments {
		if l.cfg.SyncOnAppend {
			_ = seg.file.Sync()
		}
		if err := seg.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if seg.idxFile != nil {
			_ = seg.idxFile.Sync()
			if err := seg.idxFile.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (l *segmentedLog) ensureActiveSegmentLocked() *segment {
	if len(l.segments) == 0 {
		_ = l.createSegment(l.opts.BaseOffset)
	}
	return l.segments[len(l.segments)-1]
}

func (l *segmentedLog) enforceRetentionLocked() {
	// Age-based retention
	now := time.Now()
	for len(l.segments) > 1 && l.cfg.SegmentMaxAge > 0 {
		oldest := l.segments[0]
		if now.Sub(oldest.createdAt) <= l.cfg.SegmentMaxAge {
			break
		}
		l.removeOldestSegmentLocked()
	}
	// Size-based retention (per partition)
	if l.cfg.MaxLogBytes > 0 {
		for l.totalSizeLocked() > l.cfg.MaxLogBytes && len(l.segments) > 1 {
			l.removeOldestSegmentLocked()
		}
	}
}

func (l *segmentedLog) totalSizeLocked() int64 {
	var total int64
	for _, seg := range l.segments {
		total += seg.size
	}
	return total
}

func (l *segmentedLog) removeOldestSegmentLocked() {
	if len(l.segments) == 0 {
		return
	}
	oldest := l.segments[0]
	_ = oldest.file.Close()
	if oldest.idxFile != nil {
		_ = oldest.idxFile.Close()
	}
	_ = os.Remove(oldest.path)
	if oldest.idxPath != "" {
		_ = os.Remove(oldest.idxPath)
	}
	l.segments = l.segments[1:]
	if len(l.segments) > 0 {
		l.startOffset = l.segments[0].baseOffset
	} else {
		l.startOffset = l.nextOffset
	}
}

func (l *segmentedLog) loadOrRebuildIndex(seg *segment) error {
	idxPath := seg.idxPath
	if f, entries, err := loadIndex(idxPath, seg.baseOffset); err == nil {
		seg.idxFile = f
		seg.idx = entries
		return nil
	}
	// Rebuild index from log if missing or corrupt.
	if err := rebuildIndex(seg.path, idxPath, seg.baseOffset, l.cfg.IndexInterval); err != nil {
		return err
	}
	f, entries, err := loadIndex(idxPath, seg.baseOffset)
	if err != nil {
		return err
	}
	seg.idxFile = f
	seg.idx = entries
	return nil
}

func rebuildIndex(logPath, idxPath string, base api.Offset, interval int) error {
	if interval <= 0 {
		interval = 1024
	}
	logFile, err := os.OpenFile(logPath, os.O_RDONLY, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer idxFile.Close()
	if err := binary.Write(idxFile, binary.LittleEndian, int64(base)); err != nil {
		return err
	}
	var (
		readerOffset int64
		count        int
		expected     = base
		headerBuf    = make([]byte, 4)
	)
	for {
		if _, err := logFile.ReadAt(headerBuf, readerOffset); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		size := binary.LittleEndian.Uint32(headerBuf)
		if size == 0 {
			return nil
		}
		recordBuf := make([]byte, size)
		if _, err := logFile.ReadAt(recordBuf, readerOffset+4); err != nil {
			return nil
		}
		if err := validateRecord(recordBuf, expected); err != nil {
			return nil
		}
		if count%interval == 0 {
			rel := int64(expected - base)
			if err := binary.Write(idxFile, binary.LittleEndian, rel); err != nil {
				return err
			}
			if err := binary.Write(idxFile, binary.LittleEndian, readerOffset); err != nil {
				return err
			}
		}
		readerOffset += int64(4 + size)
		count++
		expected++
	}
}

func loadIndex(idxPath string, base api.Offset) (*os.File, []indexEntry, error) {
	f, err := os.OpenFile(idxPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, err
	}
	var hdr int64
	if err := binary.Read(f, binary.LittleEndian, &hdr); err != nil || api.Offset(hdr) != base {
		_ = f.Close()
		return nil, nil, fmt.Errorf("invalid index header")
	}
	var entries []indexEntry
	for {
		var rel, pos int64
		if err := binary.Read(f, binary.LittleEndian, &rel); err != nil {
			break
		}
		if err := binary.Read(f, binary.LittleEndian, &pos); err != nil {
			break
		}
		entries = append(entries, indexEntry{RelativeOffset: rel, Position: pos})
	}
	return f, entries, nil
}

func scanSegment(f *os.File, base api.Offset) (api.Offset, int64, error) {
	var (
		nextOffset   = base
		validBytes   int64
		headerBuf    = make([]byte, 4)
		readerOffset int64
	)
	for {
		if _, err := f.ReadAt(headerBuf, readerOffset); err != nil {
			if errors.Is(err, io.EOF) {
				return nextOffset, validBytes, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nextOffset, validBytes, nil
			}
			return nextOffset, validBytes, nil
		}
		size := binary.LittleEndian.Uint32(headerBuf)
		if size == 0 {
			return nextOffset, validBytes, nil
		}
		recordBuf := make([]byte, size)
		if _, err := f.ReadAt(recordBuf, readerOffset+4); err != nil {
			// partial tail -> stop and truncate
			return nextOffset, validBytes, nil
		}
		if err := validateRecord(recordBuf, nextOffset); err != nil {
			return nextOffset, validBytes, nil
		}
		consumed := int64(4 + size)
		validBytes += consumed
		readerOffset += consumed
		nextOffset++
	}
}

func validateRecord(data []byte, expectedOffset api.Offset) error {
	if len(data) < 4+8+8+4+4+4 { // crc + offset + ts + keyLen + valueLen + headersCount
		return fmt.Errorf("record too small")
	}
	crc := binary.LittleEndian.Uint32(data[:4])
	calculated := crc32.Checksum(data[4:], crcTable)
	if crc != calculated {
		return fmt.Errorf("crc mismatch")
	}
	offset := api.Offset(binary.LittleEndian.Uint64(data[4:]))
	if offset != expectedOffset {
		return fmt.Errorf("offset mismatch: got %d expected %d", offset, expectedOffset)
	}
	return nil
}

func readFromSegment(ctx context.Context, seg *segment, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	var res []api.Record
	startPos := int64(0)
	if pos, ok := lookupIndex(seg, offset); ok {
		startPos = pos
	}
	reader := io.NewSectionReader(seg.file, startPos, seg.size-startPos)
	var consumed int64
	for {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		default:
		}
		var sizeBuf [4]byte
		if _, err := reader.Read(sizeBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return res, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return res, nil
			}
			return res, err
		}
		size := binary.LittleEndian.Uint32(sizeBuf[:])
		if size == 0 {
			return res, nil
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return res, nil
		}
		rec, err := decodeRecord(data)
		if err != nil {
			return res, err
		}
		if err := validateRecord(data, rec.Offset); err != nil {
			return res, err
		}
		consumed += int64(4 + size)
		if rec.Offset < offset {
			continue
		}
		res = append(res, rec)
		if maxBytes > 0 {
			maxBytes -= int32(len(rec.Value))
			if maxBytes <= 0 {
				return res, nil
			}
		}
		if consumed+startPos >= seg.size {
			return res, nil
		}
	}
}

func seekToOffset(seg *segment, target api.Offset) (int64, error) {
	reader := io.NewSectionReader(seg.file, 0, seg.size)
	var pos int64
	for {
		var sizeBuf [4]byte
		_, err := reader.Read(sizeBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return pos, nil
			}
			return pos, err
		}
		size := binary.LittleEndian.Uint32(sizeBuf[:])
		if size == 0 {
			return pos, nil
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return pos, err
		}
		rec, err := decodeRecord(data)
		if err != nil {
			return pos, err
		}
		entrySize := int64(4 + size)
		if rec.Offset >= target {
			return pos, nil
		}
		pos += entrySize
	}
}

func lookupIndex(seg *segment, target api.Offset) (int64, bool) {
	if len(seg.idx) == 0 {
		return 0, false
	}
	relTarget := int64(target - seg.baseOffset)
	lo, hi := 0, len(seg.idx)-1
	best := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if seg.idx[mid].RelativeOffset == relTarget {
			best = mid
			break
		}
		if seg.idx[mid].RelativeOffset < relTarget {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best >= 0 {
		return seg.idx[best].Position, true
	}
	return 0, false
}

func encodeRecord(r api.Record) ([]byte, error) {
	var buf bytes.Buffer
	// placeholder for length
	if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, err
	}
	// placeholder for crc
	if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint64(r.Offset)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, r.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	writeBytes := func(b []byte) error {
		if b == nil {
			return binary.Write(&buf, binary.LittleEndian, int32(-1))
		}
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(b))); err != nil {
			return err
		}
		if len(b) > 0 {
			_, err := buf.Write(b)
			return err
		}
		return nil
	}
	if err := writeBytes(r.Key); err != nil {
		return nil, err
	}
	if err := writeBytes(r.Value); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(r.Headers))); err != nil {
		return nil, err
	}
	for _, h := range r.Headers {
		if err := writeBytes([]byte(h.Key)); err != nil {
			return nil, err
		}
		if err := writeBytes(h.Value); err != nil {
			return nil, err
		}
	}
	recordBytes := buf.Bytes()
	// recordSize excludes the length prefix, includes crc+payload.
	recordSize := uint32(len(recordBytes) - 4)
	binary.LittleEndian.PutUint32(recordBytes[0:4], recordSize)
	crc := crc32.Checksum(recordBytes[8:], crcTable)
	binary.LittleEndian.PutUint32(recordBytes[4:8], crc)
	return recordBytes, nil
}

func decodeRecord(data []byte) (api.Record, error) {
	var r api.Record
	if len(data) < 4+8+8+4+4+4 {
		return r, fmt.Errorf("record too small")
	}
	r.CRC32C = binary.LittleEndian.Uint32(data[:4])
	offset := binary.LittleEndian.Uint64(data[4:])
	r.Offset = api.Offset(offset)
	ts := int64(binary.LittleEndian.Uint64(data[12:]))
	r.Timestamp = time.Unix(0, ts)
	idx := 20
	readBytes := func() ([]byte, error) {
		if idx+4 > len(data) {
			return nil, fmt.Errorf("invalid length")
		}
		l := int(int32(binary.LittleEndian.Uint32(data[idx : idx+4])))
		idx += 4
		if l < 0 {
			return nil, nil
		}
		if idx+l > len(data) {
			return nil, fmt.Errorf("invalid length")
		}
		b := data[idx : idx+l]
		idx += l
		return b, nil
	}
	key, err := readBytes()
	if err != nil {
		return r, err
	}
	val, err := readBytes()
	if err != nil {
		return r, err
	}
	r.Key = key
	r.Value = val
	if idx+4 > len(data) {
		return r, fmt.Errorf("invalid header count")
	}
	hCount := int(binary.LittleEndian.Uint32(data[idx : idx+4]))
	idx += 4
	if hCount < 0 {
		return r, fmt.Errorf("invalid header count")
	}
	r.Headers = make([]api.Header, 0, hCount)
	for i := 0; i < hCount; i++ {
		k, err := readBytes()
		if err != nil {
			return r, err
		}
		v, err := readBytes()
		if err != nil {
			return r, err
		}
		r.Headers = append(r.Headers, api.Header{Key: string(k), Value: v})
	}
	return r, nil
}
