package mqtt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeBroker struct {
	mu          sync.Mutex
	records     map[string]map[int][]api.Record
	fetches     int
	fetchErr    error
	commitDelay time.Duration
	produced    int
	committed   map[string]map[string]map[int]api.Offset
	commits     []api.Offset
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		records:   make(map[string]map[int][]api.Record),
		committed: make(map[string]map[string]map[int]api.Offset),
	}
}

func (b *fakeBroker) Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error) {
	_ = ctx

	b.mu.Lock()
	defer b.mu.Unlock()

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

	b.mu.Lock()
	defer b.mu.Unlock()

	b.fetches++
	if b.fetchErr != nil {
		return nil, b.fetchErr
	}

	parts := b.records[topic][partition]
	if int(offset) >= len(parts) {
		return []api.Record{}, nil
	}

	return append([]api.Record{}, parts[offset:]...), nil
}

func (b *fakeBroker) ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error) {
	_ = ctx

	b.mu.Lock()
	defer b.mu.Unlock()

	parts := b.records[topic][partition]
	if len(parts) == 0 {
		return 0, -1, nil
	}

	return 0, api.Offset(len(parts) - 1), nil
}

func (b *fakeBroker) Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error) {
	_ = ctx

	b.mu.Lock()
	defer b.mu.Unlock()

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
	_ = group
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
	if b.commitDelay > 0 {
		timer := time.NewTimer(b.commitDelay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.committed[group]; !ok {
		b.committed[group] = make(map[string]map[int]api.Offset)
	}

	if _, ok := b.committed[group][topic]; !ok {
		b.committed[group][topic] = make(map[int]api.Offset)
	}

	b.committed[group][topic][partition] = offset
	b.commits = append(b.commits, offset)

	return nil
}

func (b *fakeBroker) FetchCommitted(ctx context.Context, group, topic string, partition int) (api.Offset, error) {
	_ = ctx

	b.mu.Lock()
	defer b.mu.Unlock()

	if off, ok := b.committed[group][topic][partition]; ok {
		return off, nil
	}

	return -1, fmt.Errorf("not found")
}

func (b *fakeBroker) appendRecord(topic string, partition int, offset api.Offset, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.records[topic]; !ok {
		b.records[topic] = make(map[int][]api.Record)
	}

	b.records[topic][partition] = append(b.records[topic][partition], api.Record{
		Offset: offset,
		Value:  append([]byte(nil), value...),
	})
}

func (b *fakeBroker) recordCount(topic string, partition int) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.records[topic][partition])
}

func (b *fakeBroker) fetchCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.fetches
}

func (b *fakeBroker) committedOffset(group, topic string, partition int) (api.Offset, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	parts, ok := b.committed[group]
	if !ok {
		return 0, false
	}

	topicOffsets, ok := parts[topic]
	if !ok {
		return 0, false
	}

	off, ok := topicOffsets[partition]

	return off, ok
}

func (b *fakeBroker) commitCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.commits)
}

func (b *fakeBroker) producedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.produced
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
	defer mustCloseConn(t, client, "client")
	defer mustCloseConn(t, server, "server")

	go srv.handleConnection(server)

	sendPacket := func(p []byte) {
		if _, err := client.Write(p); err != nil {
			t.Fatalf("write packet: %v", err)
		}
	}
	readPacketType := func() (byte, []byte, error) {
		if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return 0, nil, err
		}

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
	if err := writeString(connectBody, "MQTT"); err != nil {
		t.Fatalf("write protocol name: %v", err)
	}

	connectBody.WriteByte(4)
	connectBody.WriteByte(0b00000010) // clean start
	connectBody.Write([]byte{0, 10})  // keepalive

	if err := writeString(connectBody, "client-1"); err != nil {
		t.Fatalf("write client id: %v", err)
	}

	connectHeader := make([]byte, 0, 1+4)
	connectHeader = append(connectHeader, packetTypeCONNECT<<4)
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

	if err := writeString(subBody, "t/1"); err != nil {
		t.Fatalf("write subscribe topic: %v", err)
	}

	subBody.WriteByte(0) // QoS0

	subHeader := make([]byte, 0, 1+4)
	subHeader = append(subHeader, (packetTypeSUBSCRIBE<<4)|0x02)
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
	b.appendRecord("t/1", 0, 0, []byte("hello"))
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

		topic, err := readString(buf)
		if err != nil {
			t.Fatalf("decode publish topic: %v", err)
		}

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
	if err := writeString(pubBody, "t/1"); err != nil {
		t.Fatalf("write publish topic: %v", err)
	}

	pubBody.Write([]byte{0, 10}) // packet ID
	pubBody.WriteString("from-client")

	pubHeader := make([]byte, 0, 1+4)
	pubHeader = append(pubHeader, (packetTypePUBLISH<<4)|(qos1<<1))
	pubHeader = append(pubHeader, encodeRemainingLength(pubBody.Len())...)
	sendPacket(append(pubHeader, pubBody.Bytes()...))

	for {
		tp, _, err = readPacketType()
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

	var (
		lastType byte
		lastErr  error
	)

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

	waitForCondition(
		t,
		2*time.Second,
		func() bool { return b.recordCount("t/1", 0) >= 2 && b.fetchCount() > 0 },
		"expected broker to store produced record and run fetch loop",
	)

	committed, ok := b.committedOffset("client-1", "t/1", 0)
	if !ok {
		t.Fatalf("expected committed offset")
	}

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
		if err := writeString(body, "MQTT"); err != nil {
			t.Fatalf("write protocol name: %v", err)
		}

		body.WriteByte(4)

		flags := byte(0)
		if cleanStart {
			flags = 0b00000010
		}

		body.WriteByte(flags)
		body.Write([]byte{0, 10})

		if err := writeString(body, "client-1"); err != nil {
			t.Fatalf("write client id: %v", err)
		}

		header := make([]byte, 0, 1+4)
		header = append(header, packetTypeCONNECT<<4)
		header = append(header, encodeRemainingLength(body.Len())...)

		return append(header, body.Bytes()...)
	}

	subscribe := func(conn net.Conn) error {
		subBody := &bytes.Buffer{}
		subBody.Write([]byte{0, 1})

		if err := writeString(subBody, "topic"); err != nil {
			return err
		}

		subBody.WriteByte(0) // QoS0

		subHeader := make([]byte, 0, 1+4)
		subHeader = append(subHeader, (packetTypeSUBSCRIBE<<4)|0x02)
		subHeader = append(subHeader, encodeRemainingLength(subBody.Len())...)
		_, err := conn.Write(append(subHeader, subBody.Bytes()...))

		return err
	}

	// Backlog mode (CleanStart=false): should read old messages
	client1, server1 := net.Pipe()
	defer mustCloseConn(t, client1, "client1")
	defer mustCloseConn(t, server1, "server1")

	srv, err := NewServer("localhost:0", b)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	go srv.handleConnection(server1)

	if _, err := client1.Write(makeConnect(false)); err != nil {
		t.Fatalf("write connect (client1): %v", err)
	}

	if _, _, err := readPacketTypeClient(client1); err != nil {
		t.Fatalf("connack read (client1): %v", err)
	}

	if err := subscribe(client1); err != nil {
		t.Fatalf("write subscribe (client1): %v", err)
	}

	if _, _, err := readPacketTypeClient(client1); err != nil {
		t.Fatalf("suback read (client1): %v", err)
	}
	// Expect backlog publish
	tp, body, err := readPacketTypeClient(client1)
	if err != nil || tp != packetTypePUBLISH {
		t.Fatalf("expected backlog publish, got type %d err %v", tp, err)
	}

	buf := bytes.NewBuffer(body)
	if _, err := readString(buf); err != nil {
		t.Fatalf("decode backlog publish topic: %v", err)
	}

	payload := buf.Bytes()
	if string(payload) != "old1" {
		t.Fatalf("expected old1, got %s", string(payload))
	}

	if err := client1.Close(); err != nil {
		t.Fatalf("close client1: %v", err)
	}

	if err := server1.Close(); err != nil {
		t.Fatalf("close server1: %v", err)
	}

	// Tail-only (CleanStart=true): start after latest
	client2, server2 := net.Pipe()
	defer mustCloseConn(t, client2, "client2")
	defer mustCloseConn(t, server2, "server2")

	go srv.handleConnection(server2)

	if _, err := client2.Write(makeConnect(true)); err != nil {
		t.Fatalf("write connect (client2): %v", err)
	}

	if _, _, err := readPacketTypeClient(client2); err != nil {
		t.Fatalf("connack read (client2): %v", err)
	}

	if err := subscribe(client2); err != nil {
		t.Fatalf("write subscribe (client2): %v", err)
	}

	if _, _, err := readPacketTypeClient(client2); err != nil {
		t.Fatalf("suback read (client2): %v", err)
	}
	// Append new message after subscribe
	b.appendRecord("topic", 0, 2, []byte("new"))
	// Expect to receive only "new"
	tp, body, err = readPacketTypeClient(client2)
	if err != nil || tp != packetTypePUBLISH {
		t.Fatalf("expected publish in tail mode, got %d err %v", tp, err)
	}

	buf = bytes.NewBuffer(body)
	if _, err := readString(buf); err != nil {
		t.Fatalf("decode tail publish topic: %v", err)
	}

	payload = buf.Bytes()
	if string(payload) != "new" {
		t.Fatalf("tail mode received unexpected payload %s", string(payload))
	}
}

func TestConsumeLoopStopsPromptlyOnCancelDuringWait(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		fetchErr error
	}{
		{name: "fetch error backoff", fetchErr: fmt.Errorf("fetch failed")},
		{name: "empty poll wait"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newFakeBroker()
			b.fetchErr = tc.fetchErr

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			state := &clientState{
				ctx:    ctx,
				cancel: cancel,
				broker: b,
				opts: ServerOptions{
					EmptyPollInterval:  time.Second,
					FetchErrorBackoff:  time.Second,
					QoS1RetryInterval:  time.Second,
					CommitRetryBackoff: time.Second,
				},
				subs:             make(map[string]subscriptionState),
				outboundQoS1Acks: make(map[uint16]outboundQoS1),
				inboundQoS1Seen:  make(map[uint16]uint64),
			}

			done := make(chan struct{})

			go func() {
				state.consumeLoop(ctx, "topic", subscriptionState{
					topic:     "topic",
					partition: 0,
					qos:       qos0,
					offset:    0,
				})
				close(done)
			}()

			waitForCondition(t, 500*time.Millisecond, func() bool { return b.fetchCount() > 0 }, "expected consume loop fetch")

			cancel()

			select {
			case <-done:
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("consume loop did not stop promptly after cancel")
			}
		})
	}
}

// readPacketTypeClient is a helper similar to the inline one in basic flow.
func readPacketTypeClient(c net.Conn) (byte, []byte, error) {
	var header [1]byte

	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return 0, nil, err
	}

	if _, err := c.Read(header[:]); err != nil {
		return 0, nil, err
	}

	tp := header[0] >> 4

	remaining, err := decodeRemainingLength(c)
	if err != nil {
		return 0, nil, err
	}

	body := make([]byte, remaining)
	if _, err := io.ReadFull(c, body); err != nil {
		return 0, nil, err
	}

	return tp, body, nil
}

func mustCloseConn(t *testing.T, conn net.Conn, name string) {
	t.Helper()

	if conn == nil {
		return
	}

	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close %s: %v", name, err)
	}
}
