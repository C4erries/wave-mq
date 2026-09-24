package netproto

import (
	"bytes"
	"testing"

	"github.com/c4erries/wave-mq/pkg/api"
)

func FuzzDecodeFrameDoesNotPanic(f *testing.F) {
	payload := []byte("hello")
	frame, _ := encodeRequestFrame(api.APIKeyProduce, 1, payload)
	f.Add(frame)
	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in decodeFrame: %v", r)
			}
		}()

		_, _, _, _ = decodeFrame(bytes.NewReader(data))
	})
}

func FuzzDecodeProduceRequestDoesNotPanic(f *testing.F) {
	req := &ProduceRequest{
		Topic:     "t",
		Partition: 0,
		Records:   []api.Record{{Value: []byte("v")}},
	}
	payload, _ := encodeProduceRequest(req)
	f.Add(payload)
	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in decodeProduceRequest: %v", r)
			}
		}()

		_, _ = decodeProduceRequest(data)
	})
}
