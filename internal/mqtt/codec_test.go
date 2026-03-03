package mqtt

import (
	"bytes"
	"testing"
)

func TestConnectRoundTrip(t *testing.T) {
	orig := &ConnectPacket{
		ClientID:     "c1",
		KeepAliveSec: 10,
		CleanStart:   true,
		Username:     "u",
		AuthData:     []byte("p"),
	}
	buf := &bytes.Buffer{}
	header := make([]byte, 0, 1+4)
	header = append(header, packetTypeCONNECT<<4)
	body := &bytes.Buffer{}
	if err := writeString(body, "MQTT"); err != nil {
		t.Fatalf("write protocol name: %v", err)
	}
	body.WriteByte(4) // protocol level
	body.WriteByte(0b11000010)
	body.Write([]byte{0, 10})
	if err := writeString(body, orig.ClientID); err != nil {
		t.Fatalf("write client id: %v", err)
	}
	if err := writeString(body, orig.Username); err != nil {
		t.Fatalf("write username: %v", err)
	}
	if err := writeString(body, string(orig.AuthData)); err != nil {
		t.Fatalf("write auth data: %v", err)
	}
	header = append(header, encodeRemainingLength(body.Len())...)
	buf.Write(header)
	buf.Write(body.Bytes())

	pkt, err := readPacket(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("decode connect: %v", err)
	}

	conn, ok := pkt.(*ConnectPacket)
	if !ok {
		t.Fatalf("expected ConnectPacket")
	}

	if conn.ClientID != orig.ClientID || conn.Username != orig.Username {
		t.Fatalf("connect fields mismatch: %#v", conn)
	}
}

func TestSubscribeRoundTrip(t *testing.T) {
	body := &bytes.Buffer{}
	body.Write([]byte{0, 42})
	if err := writeString(body, "a/b"); err != nil {
		t.Fatalf("write first topic: %v", err)
	}
	body.WriteByte(0)
	if err := writeString(body, "c/d"); err != nil {
		t.Fatalf("write second topic: %v", err)
	}
	body.WriteByte(1)

	header := make([]byte, 0, 1+4)
	header = append(header, (packetTypeSUBSCRIBE<<4)|0x02)
	header = append(header, encodeRemainingLength(body.Len())...)
	header = append(header, body.Bytes()...)
	all := header

	pkt, err := readPacket(bytes.NewReader(all))
	if err != nil {
		t.Fatalf("decode subscribe: %v", err)
	}

	sub, ok := pkt.(*SubscribePacket)
	if !ok {
		t.Fatalf("expected SubscribePacket")
	}

	if len(sub.Topics) != 2 || sub.Topics[1].QoS != 1 {
		t.Fatalf("subscribe mismatch: %#v", sub)
	}
}

func TestPublishRoundTripQoS1(t *testing.T) {
	orig := &PublishPacket{Topic: "t", QoS: 1, PacketID: 5, Payload: []byte("hi")}
	body := &bytes.Buffer{}
	if err := writeString(body, orig.Topic); err != nil {
		t.Fatalf("write topic: %v", err)
	}
	body.Write([]byte{0, 5})
	body.Write(orig.Payload)

	header := make([]byte, 0, 1+4)
	header = append(header, (packetTypePUBLISH<<4)|(orig.QoS<<1))
	header = append(header, encodeRemainingLength(body.Len())...)
	header = append(header, body.Bytes()...)
	all := header

	pkt, err := readPacket(bytes.NewReader(all))
	if err != nil {
		t.Fatalf("decode publish: %v", err)
	}

	pub, ok := pkt.(*PublishPacket)
	if !ok {
		t.Fatalf("expected PublishPacket")
	}

	if pub.QoS != 1 || pub.PacketID != 5 || string(pub.Payload) != "hi" {
		t.Fatalf("publish mismatch: %#v", pub)
	}
}

func TestPublishRoundTripQoS1Duplicate(t *testing.T) {
	orig := &PublishPacket{
		Topic:     "t",
		QoS:       1,
		Duplicate: true,
		PacketID:  9,
		Payload:   []byte("dup"),
	}

	buf := &bytes.Buffer{}
	if err := writePublish(buf, orig); err != nil {
		t.Fatalf("encode publish: %v", err)
	}

	pkt, err := readPacket(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("decode publish: %v", err)
	}

	pub, ok := pkt.(*PublishPacket)
	if !ok {
		t.Fatalf("expected PublishPacket")
	}

	if !pub.Duplicate || pub.PacketID != 9 || string(pub.Payload) != "dup" {
		t.Fatalf("publish duplicate mismatch: %#v", pub)
	}
}

func TestPingReq(t *testing.T) {
	header := []byte{packetTypePINGREQ << 4, 0}

	pkt, err := readPacket(bytes.NewReader(header))
	if err != nil {
		t.Fatalf("decode pingreq: %v", err)
	}

	if _, ok := pkt.(*PingreqPacket); !ok {
		t.Fatalf("expected PingreqPacket")
	}
}
