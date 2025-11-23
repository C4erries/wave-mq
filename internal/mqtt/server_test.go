package mqtt

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeBroker struct {
	records  map[string]map[int][]api.Record
	fetches  int
	produced int
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{records: make(map[string]map[int][]api.Record)}
}

func (b *fakeBroker) Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error) {
	_ = ctx
	if _, ok := b.records[topic]; !ok {
		b.records[topic] = make(map[int][]api.Record)
	}
	list := b.records[topic][partition]
	base := api.Offset(len(list))
	for i := range records {
		records[i].Offset = base + api.Offset(i)
		list = append(list, records[i])
	}
	b.records[topic][partition] = list
	b.produced++
	return base, nil
}

func (b *fakeBroker) Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	_ = ctx
	_ = maxBytes
	b.fetches++
	parts := b.records[topic][partition]
	if int(offset) >= len(parts) {
		return []api.Record{}, nil
	}
	return append([]api.Record{}, parts[offset:]...), nil
}

func (b *fakeBroker) ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error) {
	_ = ctx
	parts := b.records[topic][partition]
	if len(parts) == 0 {
		return 0, -1, nil
	}
	return 0, api.Offset(len(parts) - 1), nil
}

func (b *fakeBroker) Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error) {
	_ = ctx
	var res []api.PartitionMetadata
	for topic, parts := range b.records {
		if len(topics) > 0 && topics[0] != topic {
			continue
		}
		for pid := range parts {
			res = append(res, api.PartitionMetadata{
				Replica: api.PartitionReplica{
					Topic:     topic,
					Partition: pid,
					BrokerID:  1,
					Role:      api.RoleLeader,
				},
				StartOffset:   0,
				HighWatermark: api.Offset(len(parts[pid]) - 1),
			})
		}
	}
	return res, nil
}

func TestMQTTServerBasicFlow(t *testing.T) {
	b := newFakeBroker()
	b.records["t/1"] = map[int][]api.Record{
		0: {},
	}
	srv, err := NewServer("localhost:0", b)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go srv.handleConnection(server)

	sendPacket := func(p []byte) {
		client.Write(p)
	}
	readPacketType := func() (byte, []byte, error) {
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		var header [1]byte
		if _, err := client.Read(header[:]); err != nil {
			return 0, nil, err
		}
		tp := header[0] >> 4
		remaining, err := decodeRemainingLength(client)
		if err != nil {
			return 0, nil, err
		}
		body := make([]byte, remaining)
		if _, err := client.Read(body); err != nil {
			return 0, nil, err
		}
		return tp, body, nil
	}

	// CONNECT
	connectBody := &bytes.Buffer{}
	_ = writeString(connectBody, "MQTT")
	connectBody.WriteByte(4)
	connectBody.WriteByte(0b00000010) // clean start
	connectBody.Write([]byte{0, 10})  // keepalive
	_ = writeString(connectBody, "client-1")
	connectHeader := []byte{packetTypeCONNECT << 4}
	connectHeader = append(connectHeader, encodeRemainingLength(connectBody.Len())...)
	sendPacket(append(connectHeader, connectBody.Bytes()...))
	tp, body, err := readPacketType()
	if err != nil {
		t.Fatalf("connack read: %v", err)
	}
	if tp != packetTypeCONNACK || len(body) < 2 || body[1] != 0 {
		t.Fatalf("unexpected CONNACK: type=%d body=%v", tp, body)
	}

	// SUBSCRIBE to t/1
	subBody := &bytes.Buffer{}
	subBody.Write([]byte{0, 1}) // packet ID
	_ = writeString(subBody, "t/1")
	subBody.WriteByte(0) // QoS0
	subHeader := []byte{(packetTypeSUBSCRIBE << 4) | 0x02}
	subHeader = append(subHeader, encodeRemainingLength(subBody.Len())...)
	sendPacket(append(subHeader, subBody.Bytes()...))
	tp, body, err = readPacketType()
	if err != nil {
		t.Fatalf("suback read: %v", err)
	}
	if tp != packetTypeSUBACK || len(body) < 3 || body[2] == 0x80 {
		t.Fatalf("unexpected SUBACK: %v", body)
	}

	// Append record to broker so fetch loop delivers it.
	b.records["t/1"][0] = append(b.records["t/1"][0], api.Record{Offset: 0, Value: []byte("hello")})
	// Expect PUBLISH from server with "hello"
	received := false
	for i := 0; i < 20 && !received; i++ {
		tp, body, err = readPacketType()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			t.Fatalf("publish read: %v", err)
		}
		if tp != packetTypePUBLISH {
			continue
		}
		buf := bytes.NewBuffer(body)
		topic, _ := readString(buf)
		payload := buf.Bytes()
		if topic == "t/1" && string(payload) == "hello" {
			received = true
		}
	}
	if !received {
		t.Fatalf("did not receive publish")
	}

	// Send PUBLISH QoS1 to server
	pubBody := &bytes.Buffer{}
	_ = writeString(pubBody, "t/1")
	pubBody.Write([]byte{0, 10}) // packet ID
	pubBody.Write([]byte("from-client"))
	pubHeader := []byte{(packetTypePUBLISH << 4) | (qos1 << 1)}
	pubHeader = append(pubHeader, encodeRemainingLength(pubBody.Len())...)
	sendPacket(append(pubHeader, pubBody.Bytes()...))
	tp, body, err = readPacketType()
	if err != nil {
		t.Fatalf("puback read: %v", err)
	}
	if tp != packetTypePUBACK {
		t.Fatalf("expected PUBACK got %d", tp)
	}

	// PINGREQ/PINGRESP
	sendPacket([]byte{packetTypePINGREQ << 4, 0})
	tp, _, err = readPacketType()
	if err != nil {
		t.Fatalf("pingresp read: %v", err)
	}
	if tp != packetTypePINGRESP {
		t.Fatalf("expected PINGRESP got %d", tp)
	}

	// Give some time for broker produce to be called
	time.Sleep(50 * time.Millisecond)
	if len(b.records["t/1"][0]) < 2 {
		t.Fatalf("expected broker to store produced record")
	}
	if b.fetches == 0 {
		t.Fatalf("fetch loop did not run")
	}
}
