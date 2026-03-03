package storage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "t1", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	defer func() {
		_ = log.Close()
		_ = m.Close()
	}()

	ctx := context.Background()
	records := []api.Record{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}

	base, err := log.AppendBatch(ctx, records)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	if base != 0 {
		t.Fatalf("expected base offset 0, got %d", base)
	}

	got, err := log.Read(ctx, 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(got))
	}

	for i, r := range got {
		if r.Offset != api.Offset(i) {
			t.Fatalf("record %d offset mismatch: %d", i, r.Offset)
		}

		if !bytes.Equal(r.Value, records[i].Value) {
			t.Fatalf("record %d value mismatch: %s", i, string(r.Value))
		}
	}

	gotTail, err := log.Read(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read tail: %v", err)
	}

	if len(gotTail) != 1 || gotTail[0].Offset != 1 {
		t.Fatalf("expected one record from offset 1")
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 128,
		SyncOnAppend:    true,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "t1", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	defer func() {
		_ = log.Close()
		_ = m.Close()
	}()

	ctx := context.Background()

	for i := 0; i < 6; i++ {
		val := []byte(strings.Repeat("payload", 10))
		if _, err := log.Append(ctx, api.Record{Value: val}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	logDir := filepath.Join(dir, "t1", "0")

	entries, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}

	var logFiles int

	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			logFiles++
		}
	}

	if logFiles < 2 {
		t.Fatalf("expected rotation to create multiple segments, got %d", logFiles)
	}

	all, err := log.Read(ctx, 0, 0)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if len(all) != 6 {
		t.Fatalf("expected 6 records, got %d", len(all))
	}

	for i, r := range all {
		if r.Offset != api.Offset(i) {
			t.Fatalf("offset mismatch at %d: %d", i, r.Offset)
		}
	}
}

func TestRecoverTruncatesCorruptTail(t *testing.T) {
	dir := t.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 1024,
		SyncOnAppend:    true,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "t1", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	defer m.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := log.Append(ctx, api.Record{Value: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	logPath := filepath.Join(dir, "t1", "0", "00000000000000000000.log")

	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}

	if _, err := f.WriteString("junk"); err != nil {
		t.Fatalf("corrupt tail: %v", err)
	}

	_ = f.Close()

	if err := m.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	log, err = m.OpenLog(LogOptions{Topic: "t1", Partition: 0})
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer log.Close()

	records, err := log.Read(ctx, 0, 0)
	if err != nil {
		t.Fatalf("read after recover: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 valid records after recovery, got %d", len(records))
	}

	for i, r := range records {
		if r.Offset != api.Offset(i) {
			t.Fatalf("offset mismatch after recover at %d: %d", i, r.Offset)
		}
	}
}

func TestAppendAfterReopenPreservesExistingSegmentData(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   1,
	}

	first, err := NewManager(cfg)
	if err != nil {
		t.Fatalf("first manager: %v", err)
	}

	log, err := first.OpenLog(LogOptions{Topic: "t1", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := log.Append(ctx, api.Record{Value: []byte("before-" + strconv.Itoa(i))}); err != nil {
			t.Fatalf("append before %d: %v", i, err)
		}
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close first log: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first manager: %v", err)
	}

	second, err := NewManager(cfg)
	if err != nil {
		t.Fatalf("second manager: %v", err)
	}
	defer second.Close()

	log, err = second.OpenLog(LogOptions{Topic: "t1", Partition: 0})
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer log.Close()

	for i := 0; i < 2; i++ {
		if _, err := log.Append(ctx, api.Record{Value: []byte("after-" + strconv.Itoa(i))}); err != nil {
			t.Fatalf("append after %d: %v", i, err)
		}
	}

	records, err := log.Read(ctx, 0, 0)
	if err != nil {
		t.Fatalf("read after reopen append: %v", err)
	}

	if len(records) != 7 {
		t.Fatalf("expected 7 records, got %d", len(records))
	}

	for i, rec := range records {
		if rec.Offset != api.Offset(i) {
			t.Fatalf("offset mismatch at %d: got %d", i, rec.Offset)
		}
	}

	for i := 0; i < 5; i++ {
		got := string(records[i].Value)
		want := "before-" + strconv.Itoa(i)
		if got != want {
			t.Fatalf("before record %d mismatch: got %q want %q", i, got, want)
		}
	}
	for i := 0; i < 2; i++ {
		got := string(records[5+i].Value)
		want := "after-" + strconv.Itoa(i)
		if got != want {
			t.Fatalf("after record %d mismatch: got %q want %q", i, got, want)
		}
	}
}

func TestIndexRebuildAndSeek(t *testing.T) {
	dir := t.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 1 << 20,
		IndexInterval:   2,
		SyncOnAppend:    false,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "t", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	defer func() {
		log.Close()
		m.Close()
	}()
	defer func() {
		log.Close()
		m.Close()
	}()
	defer func() {
		log.Close()
		m.Close()
	}()
	defer func() {
		log.Close()
		m.Close()
	}()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := log.Append(ctx, api.Record{Value: []byte(strings.Repeat("x", 10))}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	idxPath := filepath.Join(dir, "t", "0", "00000000000000000000.idx")
	if _, err := os.Stat(idxPath); err != nil {
		t.Fatalf("index not created: %v", err)
	}

	recs, err := log.Read(ctx, 5, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(recs) == 0 || recs[0].Offset != 5 {
		t.Fatalf("expected offset 5, got %+v", recs)
	}

	// Delete index and reopen, ensure it is rebuilt.
	log.Close()

	if err := os.Remove(idxPath); err != nil {
		t.Fatalf("remove idx: %v", err)
	}

	log, err = m.OpenLog(LogOptions{Topic: "t", Partition: 0})
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	defer log.Close()

	if _, err := os.Stat(idxPath); err != nil {
		t.Fatalf("index not rebuilt: %v", err)
	}

	recs, err = log.Read(ctx, 7, 0)
	if err != nil || len(recs) == 0 || recs[0].Offset != 7 {
		t.Fatalf("read after rebuild failed: %+v err=%v", recs, err)
	}
}

func TestRetentionBySize(t *testing.T) {
	dir := t.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 128,
		MaxLogBytes:     256,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "t", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	defer func() {
		log.Close()
		m.Close()
	}()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := log.Append(ctx, api.Record{Value: []byte(strings.Repeat("a", 64))}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	logDir := filepath.Join(dir, "t", "0")
	entries, _ := os.ReadDir(logDir)

	var logFiles int

	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logFiles++
		}
	}

	if logFiles >= 4 { // should have deleted some
		t.Fatalf("retention by size did not delete segments, log files=%d", logFiles)
	}

	recs, err := log.Read(ctx, 0, 0)
	if err != nil {
		t.Fatalf("read after retention: %v", err)
	}

	if len(recs) == 0 {
		t.Fatalf("expected data after retention")
	}

	log.Close()
	m.Close()
}

func TestRetentionByAge(t *testing.T) {
	dir := t.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 128,
		SegmentMaxAge:   time.Millisecond,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "t", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := log.Append(ctx, api.Record{Value: []byte(strings.Repeat("b", 64))}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// Touch oldest segment to be old
	dirPath := filepath.Join(dir, "t", "0")

	files, err := filepath.Glob(filepath.Join(dirPath, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no log files to age")
	}

	segPath := files[0]
	oldTime := time.Now().Add(-time.Second)

	log.Close()

	if err := os.Chtimes(segPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	log, err = m.OpenLog(LogOptions{Topic: "t", Partition: 0})
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}

	time.Sleep(2 * time.Millisecond)

	if _, err := log.Append(ctx, api.Record{Value: []byte("new")}); err != nil {
		t.Fatalf("append new: %v", err)
	}

	entries, _ := os.ReadDir(filepath.Join(dir, "t", "0"))

	var logFiles int

	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logFiles++
		}
	}

	if logFiles == 0 {
		t.Fatalf("all segments removed unexpectedly")
	}

	log.Close()
	m.Close()
}

func TestStartOffsetAfterRetention(t *testing.T) {
	dir := t.TempDir()

	m, err := NewManager(Config{
		DataDir:         dir,
		MaxSegmentBytes: 64,
		MaxLogBytes:     100,
		IndexInterval:   1,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	log, err := m.OpenLog(LogOptions{Topic: "t", Partition: 0})
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := log.Append(ctx, api.Record{Value: []byte("data")}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	start := log.StartOffset()
	if start <= 0 {
		t.Fatalf("expected start offset to advance after retention, got %d", start)
	}

	recs, err := log.Read(ctx, 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(recs) == 0 || recs[0].Offset < start {
		t.Fatalf("read returned offsets before start: %v", recs)
	}

	log.Close()
	m.Close()
}
