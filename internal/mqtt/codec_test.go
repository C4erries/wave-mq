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
		Password:     []byte("p"),
	}
	buf := &bytes.Buffer{}
	header := []byte{packetTypeCONNECT << 4}
	body := &bytes.Buffer{}
	_ = writeString(body, "MQTT")
	body.WriteByte(4) // protocol level
	body.WriteByte(0b11000010)
	body.Write([]byte{0, 10})
	_ = writeString(body, orig.ClientID)
	_ = writeString(body, orig.Username)
	_ = writeString(body, string(orig.Password))
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
	_ = writeString(body, "a/b")
	body.WriteByte(0)
	_ = writeString(body, "c/d")
	body.WriteByte(1)

	header := []byte{(packetTypeSUBSCRIBE << 4) | 0x02}
	header = append(header, encodeRemainingLength(body.Len())...)
	all := append(header, body.Bytes()...)

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
	_ = writeString(body, orig.Topic)
	body.Write([]byte{0, 5})
	body.Write(orig.Payload)
	header := []byte{(packetTypePUBLISH << 4) | (orig.QoS << 1)}
	header = append(header, encodeRemainingLength(body.Len())...)
	all := append(header, body.Bytes()...)

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
