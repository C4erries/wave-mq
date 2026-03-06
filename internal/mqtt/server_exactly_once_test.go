package mqtt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestMQTTQoS1CommitOnlyAfterPuback(t *testing.T) {
	b := newFakeBroker()
	b.records["topic"] = map[int][]api.Record{
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

	mustWritePacket(t, client, buildConnectPacket("client-qos1", false))
	mustReadConnack(t, client)

	mustWritePacket(t, client, buildSubscribePacket(1, "topic", qos1))

	suback := mustReadSuback(t, client)
	if len(suback.Granted) != 1 || suback.Granted[0] != qos1 {
		t.Fatalf("unexpected SUBACK: %#v", suback.Granted)
	}

	b.appendRecord("topic", 0, 0, []byte("event-1"))

	pub := mustReadPublish(t, client)
	if pub.Topic != "topic" || pub.QoS != qos1 || string(pub.Payload) != "event-1" {
		t.Fatalf("unexpected publish packet: %#v", pub)
	}

	assertConditionStable(
		t,
		150*time.Millisecond,
		func() bool { return b.commitCount() == 0 },
		"commit happened before PUBACK",
	)

	if err := writePuback(client, &PubackPacket{PacketID: pub.PacketID}); err != nil {
		t.Fatalf("write PUBACK: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool { return b.commitCount() == 1 }, "expected commit after PUBACK")

	off, ok := b.committedOffset("client-qos1", "topic", 0)
	if !ok {
		t.Fatalf("expected committed offset")
	}

	if off != 0 {
		t.Fatalf("unexpected committed offset: %d", off)
	}
}

func TestMQTTQoS1RedeliveryAfterReconnectWithoutPuback(t *testing.T) {
	b := newFakeBroker()
	b.records["topic"] = map[int][]api.Record{
		0: {},
	}

	srv, err := NewServer("localhost:0", b)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	client1, server1 := net.Pipe()
	defer mustCloseConn(t, client1, "client1")
	defer mustCloseConn(t, server1, "server1")

	go srv.handleConnection(server1)

	mustWritePacket(t, client1, buildConnectPacket("client-resume", false))
	mustReadConnack(t, client1)

	mustWritePacket(t, client1, buildSubscribePacket(1, "topic", qos1))
	mustReadSuback(t, client1)

	b.appendRecord("topic", 0, 0, []byte("event-1"))

	pub1 := mustReadPublish(t, client1)
	if string(pub1.Payload) != "event-1" {
		t.Fatalf("unexpected first publish payload: %q", string(pub1.Payload))
	}

	if err := client1.Close(); err != nil {
		t.Fatalf("close client1: %v", err)
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("close server1: %v", err)
	}

	assertConditionStable(
		t,
		120*time.Millisecond,
		func() bool { return b.commitCount() == 0 },
		"unexpected commits before reconnect",
	)

	client2, server2 := net.Pipe()
	defer mustCloseConn(t, client2, "client2")
	defer mustCloseConn(t, server2, "server2")

	go srv.handleConnection(server2)

	mustWritePacket(t, client2, buildConnectPacket("client-resume", false))
	mustReadConnack(t, client2)

	mustWritePacket(t, client2, buildSubscribePacket(2, "topic", qos1))
	mustReadSuback(t, client2)

	pub2 := mustReadPublish(t, client2)
	if string(pub2.Payload) != "event-1" {
		t.Fatalf("unexpected redelivery payload: %q", string(pub2.Payload))
	}

	if err := writePuback(client2, &PubackPacket{PacketID: pub2.PacketID}); err != nil {
		t.Fatalf("write PUBACK after reconnect: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool { return b.commitCount() == 1 }, "expected exactly one commit after reconnect ACK")

	off, ok := b.committedOffset("client-resume", "topic", 0)
	if !ok {
		t.Fatalf("expected committed offset")
	}

	if off != 0 {
		t.Fatalf("unexpected committed offset after reconnect: %d", off)
	}
}

func TestMQTTQoS1IncomingDuplicateIsIdempotent(t *testing.T) {
	b := newFakeBroker()
	b.records["ingest"] = map[int][]api.Record{
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

	mustWritePacket(t, client, buildConnectPacket("publisher-1", true))
	mustReadConnack(t, client)

	first := &PublishPacket{
		Topic:    "ingest",
		QoS:      qos1,
		PacketID: 7,
		Payload:  []byte("payload"),
	}
	if err := writePublish(client, first); err != nil {
		t.Fatalf("write first QoS1 publish: %v", err)
	}

	puback1 := mustReadPuback(t, client)
	if puback1.PacketID != 7 {
		t.Fatalf("unexpected first PUBACK packet id: %d", puback1.PacketID)
	}

	duplicate := &PublishPacket{
		Topic:     "ingest",
		QoS:       qos1,
		Duplicate: true,
		PacketID:  7,
		Payload:   []byte("payload"),
	}
	if err := writePublish(client, duplicate); err != nil {
		t.Fatalf("write duplicate QoS1 publish: %v", err)
	}

	puback2 := mustReadPuback(t, client)
	if puback2.PacketID != 7 {
		t.Fatalf("unexpected duplicate PUBACK packet id: %d", puback2.PacketID)
	}

	assertConditionStable(
		t,
		120*time.Millisecond,
		func() bool { return b.producedCount() == 1 },
		"expected a single produce for duplicate QoS1 publish",
	)

	if got := b.recordCount("ingest", 0); got != 1 {
		t.Fatalf("expected one stored record for duplicate QoS1 publish, got %d", got)
	}
}

func TestMQTTQoS1CommitAfterPubackOnImmediateDisconnect(t *testing.T) {
	b := newFakeBroker()
	b.commitDelay = 80 * time.Millisecond
	b.records["topic"] = map[int][]api.Record{
		0: {},
	}

	srv, err := NewServerWithOptions("localhost:0", b, ServerOptions{
		CommitFinalTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	client, server := net.Pipe()
	defer mustCloseConn(t, client, "client")
	defer mustCloseConn(t, server, "server")

	go srv.handleConnection(server)

	mustWritePacket(t, client, buildConnectPacket("client-fast-disconnect", false))
	mustReadConnack(t, client)

	mustWritePacket(t, client, buildSubscribePacket(1, "topic", qos1))
	suback := mustReadSuback(t, client)
	if len(suback.Granted) != 1 || suback.Granted[0] != qos1 {
		t.Fatalf("unexpected SUBACK: %#v", suback.Granted)
	}

	b.appendRecord("topic", 0, 0, []byte("event-1"))

	pub := mustReadPublish(t, client)
	if pub.PacketID == 0 {
		t.Fatalf("expected qos1 packet id")
	}

	if err := writePuback(client, &PubackPacket{PacketID: pub.PacketID}); err != nil {
		t.Fatalf("write PUBACK: %v", err)
	}

	mustWritePacket(t, client, []byte{packetTypeDISCONNECT << 4, 0})

	waitForCondition(
		t,
		2*time.Second,
		func() bool { return b.commitCount() == 1 },
		"expected commit after PUBACK even if client disconnects immediately",
	)

	off, ok := b.committedOffset("client-fast-disconnect", "topic", 0)
	if !ok {
		t.Fatalf("expected committed offset")
	}

	if off != 0 {
		t.Fatalf("unexpected committed offset: %d", off)
	}
}

func mustWritePacket(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()

	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write packet: %v", err)
	}
}

func mustReadConnack(t *testing.T, conn net.Conn) *ConnackPacket {
	t.Helper()

	pkt := mustReadPacketOfType(t, conn, "CONNACK")

	connack, ok := pkt.(*ConnackPacket)
	if !ok {
		t.Fatalf("expected CONNACK, got %T", pkt)
	}

	return connack
}

func mustReadSuback(t *testing.T, conn net.Conn) *SubackPacket {
	t.Helper()

	pkt := mustReadPacketOfType(t, conn, "SUBACK")

	suback, ok := pkt.(*SubackPacket)
	if !ok {
		t.Fatalf("expected SUBACK, got %T", pkt)
	}

	return suback
}

func mustReadPublish(t *testing.T, conn net.Conn) *PublishPacket {
	t.Helper()

	pkt := mustReadPacketOfType(t, conn, "PUBLISH")

	pub, ok := pkt.(*PublishPacket)
	if !ok {
		t.Fatalf("expected PUBLISH, got %T", pkt)
	}

	return pub
}

func mustReadPuback(t *testing.T, conn net.Conn) *PubackPacket {
	t.Helper()

	pkt := mustReadPacketOfType(t, conn, "PUBACK")

	puback, ok := pkt.(*PubackPacket)
	if !ok {
		t.Fatalf("expected PUBACK, got %T", pkt)
	}

	return puback
}

func mustReadPacketOfType(t *testing.T, conn net.Conn, want string) interface{} {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}

		pkt, err := readPacketForClient(conn)
		if err != nil {
			if isTimeout(err) {
				continue
			}

			t.Fatalf("read packet: %v", err)
		}

		switch want {
		case "CONNACK":
			if _, ok := pkt.(*ConnackPacket); ok {
				return pkt
			}
		case "SUBACK":
			if _, ok := pkt.(*SubackPacket); ok {
				return pkt
			}
		case "PUBLISH":
			if _, ok := pkt.(*PublishPacket); ok {
				return pkt
			}
		case "PUBACK":
			if _, ok := pkt.(*PubackPacket); ok {
				return pkt
			}
		default:
			t.Fatalf("unsupported packet type selector: %s", want)
		}
	}

	t.Fatalf("timeout waiting for %s packet", want)

	return nil
}

func readPacketForClient(conn net.Conn) (interface{}, error) {
	var fixedHeader [1]byte
	if _, err := io.ReadFull(conn, fixedHeader[:]); err != nil {
		return nil, err
	}

	packetType := fixedHeader[0] >> 4
	flags := fixedHeader[0] & 0x0F

	remaining, err := decodeRemainingLength(conn)
	if err != nil {
		return nil, err
	}

	body := make([]byte, remaining)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(body)

	switch packetType {
	case packetTypeCONNACK:
		if len(body) < 2 {
			return nil, fmt.Errorf("invalid CONNACK packet")
		}

		return &ConnackPacket{
			SessionPresent: body[0]&0x01 == 0x01,
			ReturnCode:     body[1],
		}, nil
	case packetTypeSUBACK:
		var packetID uint16
		if err := binary.Read(buf, binary.BigEndian, &packetID); err != nil {
			return nil, err
		}

		return &SubackPacket{
			PacketID: packetID,
			Granted:  append([]byte(nil), buf.Bytes()...),
		}, nil
	case packetTypePUBLISH:
		return decodePublish(buf, flags)
	case packetTypePUBACK:
		return decodePuback(buf)
	default:
		return nil, fmt.Errorf("unsupported packet type %d", packetType)
	}
}

func isTimeout(err error) bool {
	var netErr net.Error

	return errors.As(err, &netErr) && netErr.Timeout()
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, failMsg string) {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		if cond() {
			return
		}

		select {
		case <-timer.C:
			t.Fatalf("%s", failMsg)
		case <-ticker.C:
		}
	}
}

func assertConditionStable(t *testing.T, window time.Duration, cond func() bool, failMsg string) {
	t.Helper()

	timer := time.NewTimer(window)
	defer timer.Stop()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		if !cond() {
			t.Fatalf("%s", failMsg)
		}

		select {
		case <-timer.C:
			return
		case <-ticker.C:
		}
	}
}

func buildConnectPacket(clientID string, cleanStart bool) []byte {
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
	if err := writeString(body, clientID); err != nil {
		panic(err)
	}

	header := make([]byte, 0, 1+4)
	header = append(header, packetTypeCONNECT<<4)
	header = append(header, encodeRemainingLength(body.Len())...)

	return append(header, body.Bytes()...)
}

func buildSubscribePacket(packetID uint16, topic string, qos byte) []byte {
	body := &bytes.Buffer{}
	if err := binary.Write(body, binary.BigEndian, packetID); err != nil {
		panic(err)
	}
	if err := writeString(body, topic); err != nil {
		panic(err)
	}
	body.WriteByte(qos)

	header := make([]byte, 0, 1+4)
	header = append(header, (packetTypeSUBSCRIBE<<4)|0x02)
	header = append(header, encodeRemainingLength(body.Len())...)

	return append(header, body.Bytes()...)
}
