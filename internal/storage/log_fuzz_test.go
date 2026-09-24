package storage

import (
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

func FuzzDecodeRecordDoesNotPanic(f *testing.F) {
	rec := api.Record{Offset: 0, Timestamp: time.Now(), Key: []byte("k"), Value: []byte("v")}

	encoded, err := encodeRecord(rec)
	if err != nil {
		f.Fatalf("encodeRecord seed: %v", err)
	}

	if len(encoded) >= 4 {
		f.Add(encoded[4:]) // record buffer without length prefix
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in decodeRecord: %v", r)
			}
		}()

		if _, err := decodeRecord(data); err != nil {
			// Fuzz input is arbitrary; decode failures are expected.
		}

		if err := validateRecord(data, 0); err != nil {
			// Fuzz input is arbitrary; validation failures are expected.
		}
	})
}
