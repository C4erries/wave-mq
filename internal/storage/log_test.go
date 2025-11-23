package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		if string(r.Value) != string(records[i].Value) {
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
	if _, err := f.Write([]byte("junk")); err != nil {
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
