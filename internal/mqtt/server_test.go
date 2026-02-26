package mqtt

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeBroker struct {
	records   map[string]map[int][]api.Record
	fetches   int
	produced  int
	committed map[string]map[string]map[int]api.Offset
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		records:   make(map[string]map[int][]api.Record),
		committed: make(map[string]map[string]map[int]api.Offset),
	}
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
				Leader:        1,
				Replicas:      []int{1},
				ISR:           []int{1},
			})
		}
	}
	return res, nil
}

func (b *fakeBroker) JoinGroup(ctx context.Context, group, memberID string, topics []string) (map[string][]int, error) {
	_ = ctx
	_ = memberID
	assign := make(map[string][]int)
	for _, t := range topics {
		assign[t] = []int{0}
	}
	return assign, nil
}

func (b *fakeBroker) LeaveGroup(ctx context.Context, group, memberID string) error {
	_ = ctx
	_ = group
	_ = memberID
	return nil
}

func (b *fakeBroker) CommitOffset(ctx context.Context, group, topic string, partition int, offset api.Offset) error {
	_ = ctx
	if _, ok := b.committed[group]; !ok {
		b.committed[group] = make(map[string]map[int]api.Offset)
	}
	if _, ok := b.committed[group][topic]; !ok {
		b.committed[group][topic] = make(map[int]api.Offset)
	}
	b.committed[group][topic][partition] = offset
	return nil
}

func (b *fakeBroker) FetchCommitted(ctx context.Context, group, topic string, partition int) (api.Offset, error) {
	_ = ctx
	if off, ok := b.committed[group][topic][partition]; ok {
		return off, nil
	}
	return -1, fmt.Errorf("not found")
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
		if _, err := io.ReadFull(client, body); err != nil {
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
	for {
		tp, body, err = readPacketType()
		if err != nil {
			t.Fatalf("puback read: %v", err)
		}
		if tp == packetTypePUBACK {
			break
		}
		// ignore other packets (e.g., echoed publish)
	}

	// PINGREQ/PINGRESP
	sendPacket([]byte{packetTypePINGREQ << 4, 0})
	deadline := time.Now().Add(3 * time.Second)
	var lastType byte
	var lastErr error
	for {
		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("pingresp timeout: last err=%v last type=%d", lastErr, lastType)
			}
			t.Fatalf("pingresp timeout: last type=%d", lastType)
		}
		tp, _, err = readPacketType()
		if err != nil {
			lastErr = err
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			t.Fatalf("pingresp read: %v", err)
		}
		lastType = tp
		if tp == packetTypePINGRESP {
			break
		}
		// Ignore other packets (e.g., echoed publish) until PINGRESP arrives.
	}

	// Give some time for broker produce/commit to be called
	time.Sleep(50 * time.Millisecond)
	if len(b.records["t/1"][0]) < 2 {
		t.Fatalf("expected broker to store produced record")
	}
	if b.fetches == 0 {
		t.Fatalf("fetch loop did not run")
	}
	committed := b.committed["client-1"]["t/1"][0]
	if committed != 0 && committed != 1 {
		t.Fatalf("expected committed offset 0 or 1, got %d", committed)
	}
}

func TestMQTTServerTailVsBacklog(t *testing.T) {
	b := newFakeBroker()
	// preload backlog
	b.records["topic"] = map[int][]api.Record{
		0: {
			{Offset: 0, Value: []byte("old1")},
			{Offset: 1, Value: []byte("old2")},
		},
	}

	makeConnect := func(cleanStart bool) []byte {
		body := &bytes.Buffer{}
		_ = writeString(body, "MQTT")
		body.WriteByte(4)
		flags := byte(0)
		if cleanStart {
			flags = 0b00000010
		}
		body.WriteByte(flags)
		body.Write([]byte{0, 10})
		_ = writeString(body, "client-1")
		header := []byte{packetTypeCONNECT << 4}
		header = append(header, encodeRemainingLength(body.Len())...)
		return append(header, body.Bytes()...)
	}

	subscribe := func(conn net.Conn) error {
		subBody := &bytes.Buffer{}
		subBody.Write([]byte{0, 1})
		_ = writeString(subBody, "topic")
		subBody.WriteByte(0) // QoS0
		subHeader := []byte{(packetTypeSUBSCRIBE << 4) | 0x02}
		subHeader = append(subHeader, encodeRemainingLength(subBody.Len())...)
		_, err := conn.Write(append(subHeader, subBody.Bytes()...))
		return err
	}

	// Backlog mode (CleanStart=false): should read old messages
	client1, server1 := net.Pipe()
	defer client1.Close()
	defer server1.Close()
	srv, _ := NewServer("localhost:0", b)
	go srv.handleConnection(server1)
	_, _ = client1.Write(makeConnect(false))
	_, _, _ = readPacketTypeClient(client1)
	_ = subscribe(client1)
	_, _, _ = readPacketTypeClient(client1) // SUBACK
	// Expect backlog publish
	tp, body, err := readPacketTypeClient(client1)
	if err != nil || tp != packetTypePUBLISH {
		t.Fatalf("expected backlog publish, got type %d err %v", tp, err)
	}
	buf := bytes.NewBuffer(body)
	_, _ = readString(buf)
	payload := buf.Bytes()
	if string(payload) != "old1" {
		t.Fatalf("expected old1, got %s", string(payload))
	}
	client1.Close()
	server1.Close()

	// Tail-only (CleanStart=true): start after latest
	client2, server2 := net.Pipe()
	defer client2.Close()
	defer server2.Close()
	go srv.handleConnection(server2)
	_, _ = client2.Write(makeConnect(true))
	_, _, _ = readPacketTypeClient(client2)
	_ = subscribe(client2)
	_, _, _ = readPacketTypeClient(client2) // SUBACK
	// Append new message after subscribe
	b.records["topic"][0] = append(b.records["topic"][0], api.Record{Offset: 2, Value: []byte("new")})
	// Expect to receive only "new"
	tp, body, err = readPacketTypeClient(client2)
	if err != nil || tp != packetTypePUBLISH {
		t.Fatalf("expected publish in tail mode, got %d err %v", tp, err)
	}
	buf = bytes.NewBuffer(body)
	_, _ = readString(buf)
	payload = buf.Bytes()
	if string(payload) != "new" {
		t.Fatalf("tail mode received unexpected payload %s", string(payload))
	}
}

// readPacketTypeClient is a helper similar to the inline one in basic flow.
func readPacketTypeClient(c net.Conn) (byte, []byte, error) {
	var header [1]byte
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(header[:]); err != nil {
		return 0, nil, err
	}
	tp := header[0] >> 4
	remaining, err := decodeRemainingLength(c)
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, remaining)
	if _, err := c.Read(body); err != nil {
		return 0, nil, err
	}
	return tp, body, nil
}
