package mqtt

import (
	"bytes"
	"testing"
)

func FuzzReadPacketDoesNotPanic(f *testing.F) {
	connect := makeConnectPacket(true)
	f.Add(connect)
	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in readPacket: %v", r)
			}
		}()

		if _, err := readPacket(bytes.NewReader(data)); err != nil {
			return
		}
	})
}

func makeConnectPacket(cleanStart bool) []byte {
	body := &bytes.Buffer{}
	if err := writeString(body, "MQTT"); err != nil {
		panic(err)
	}
	body.WriteByte(4)

	flags := byte(0)
	if cleanStart {
		flags = 0b00000010
	}

	body.WriteByte(flags)
	body.Write([]byte{0, 10})
	if err := writeString(body, "fuzz-client"); err != nil {
		panic(err)
	}
	header := make([]byte, 0, 1+4)
	header = append(header, packetTypeCONNECT<<4)
	header = append(header, encodeRemainingLength(body.Len())...)

	return append(header, body.Bytes()...)
}
