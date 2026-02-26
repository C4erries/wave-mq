package storage

import (
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

func FuzzDecodeRecordDoesNotPanic(f *testing.F) {
	rec := api.Record{Offset: 0, Timestamp: time.Now(), Key: []byte("k"), Value: []byte("v")}

	encoded, _ := encodeRecord(rec)
	if len(encoded) >= 4 {
		f.Add(encoded[4:]) // record buffer without length prefix
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in decodeRecord: %v", r)
			}
		}()

		_, _ = decodeRecord(data)
		_ = validateRecord(data, 0)
	})
}
