package mqtt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// MQTT packet types (upper 4 bits of first byte).
const (
	packetTypeCONNECT    = 1
	packetTypeCONNACK    = 2
	packetTypePUBLISH    = 3
	packetTypePUBACK     = 4
	packetTypeSUBSCRIBE  = 8
	packetTypeSUBACK     = 9
	packetTypePINGREQ    = 12
	packetTypePINGRESP   = 13
	packetTypeDISCONNECT = 14
)

const (
	qos0 byte = 0
	qos1 byte = 1

	maxUint16 = int(^uint16(0))
)

func encodeRemainingLength(n int) []byte {
	var out []byte

	for {
		encoded := byte(n % 128) // #nosec G115 -- MQTT remaining length uses 7-bit chunks.

		n /= 128
		if n > 0 {
			encoded |= 128
		}

		out = append(out, encoded)

		if n == 0 {
			break
		}
	}

	return out
}

func decodeRemainingLength(r io.Reader) (int, error) {
	multiplier := 1
	value := 0

	for {
		var encoded byte
		if err := binary.Read(r, binary.LittleEndian, &encoded); err != nil {
			return 0, err
		}

		value += int(encoded&127) * multiplier
		if encoded&128 == 0 {
			break
		}

		multiplier *= 128
		if multiplier > 128*128*128 {
			return 0, fmt.Errorf("malformed remaining length")
		}
	}

	return value, nil
}

func writeString(w io.Writer, s string) error {
	length, err := toUint16Length(len(s))
	if err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}

	if s != "" {
		_, err := w.Write([]byte(s))
		return err
	}

	return nil
}

func readString(r io.Reader) (string, error) {
	var l uint16
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return "", err
	}

	if l == 0 {
		return "", nil
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

func readPacket(r io.Reader) (interface{}, error) {
	var fixedHeader [1]byte
	if _, err := io.ReadFull(r, fixedHeader[:]); err != nil {
		return nil, err
	}

	packetType := fixedHeader[0] >> 4
	flags := fixedHeader[0] & 0x0F

	remaining, err := decodeRemainingLength(r)
	if err != nil {
		return nil, err
	}

	body := make([]byte, remaining)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(body)

	switch packetType {
	case packetTypeCONNECT:
		return decodeConnect(buf)
	case packetTypeSUBSCRIBE:
		return decodeSubscribe(buf)
	case packetTypePUBLISH:
		return decodePublish(buf, flags)
	case packetTypePUBACK:
		return decodePuback(buf)
	case packetTypePINGREQ:
		return &PingreqPacket{}, nil
	case packetTypeDISCONNECT:
		return &DisconnectPacket{}, nil
	default:
		return nil, fmt.Errorf("unsupported packet type %d", packetType)
	}
}

func decodeConnect(r *bytes.Buffer) (*ConnectPacket, error) {
	protoName, err := readString(r)
	if err != nil {
		return nil, err
	}

	if protoName != "MQTT" {
		return nil, fmt.Errorf("unsupported protocol %s", protoName)
	}

	var protoLevel byte
	if err := binary.Read(r, binary.BigEndian, &protoLevel); err != nil {
		return nil, err
	}

	var connectFlags byte
	if err := binary.Read(r, binary.BigEndian, &connectFlags); err != nil {
		return nil, err
	}

	var keepAlive uint16
	if err := binary.Read(r, binary.BigEndian, &keepAlive); err != nil {
		return nil, err
	}

	clientID, err := readString(r)
	if err != nil {
		return nil, err
	}

	var (
		username string
		password []byte
	)

	if connectFlags&0x80 != 0 {
		username, err = readString(r)
		if err != nil {
			return nil, err
		}
	}

	if connectFlags&0x40 != 0 {
		password, err = readBytes(r)
		if err != nil {
			return nil, err
		}
	}

	return &ConnectPacket{
		ClientID:     clientID,
		KeepAliveSec: keepAlive,
		CleanStart:   connectFlags&0x02 != 0,
		Username:     username,
		AuthData:     password,
	}, nil
}

func readBytes(r io.Reader) ([]byte, error) {
	s, err := readString(r)
	if err != nil {
		return nil, err
	}

	return []byte(s), nil
}

func decodeSubscribe(r *bytes.Buffer) (*SubscribePacket, error) {
	var packetID uint16
	if err := binary.Read(r, binary.BigEndian, &packetID); err != nil {
		return nil, err
	}

	var subs []Subscription

	for r.Len() > 0 {
		filter, err := readString(r)
		if err != nil {
			return nil, err
		}

		var qos byte
		if err := binary.Read(r, binary.BigEndian, &qos); err != nil {
			return nil, err
		}

		subs = append(subs, Subscription{Filter: filter, QoS: qos & 0x03})
	}

	return &SubscribePacket{PacketID: packetID, Topics: subs}, nil
}

func decodePublish(r *bytes.Buffer, flags byte) (*PublishPacket, error) {
	topic, err := readString(r)
	if err != nil {
		return nil, err
	}

	duplicate := flags&0x08 != 0
	qos := (flags >> 1) & 0x03

	var packetID uint16
	if qos > 0 {
		if err := binary.Read(r, binary.BigEndian, &packetID); err != nil {
			return nil, err
		}
	}

	payload := r.Bytes()

	return &PublishPacket{
		Topic:     topic,
		QoS:       qos,
		Duplicate: duplicate,
		PacketID:  packetID,
		Payload:   payload,
	}, nil
}

func decodePuback(r *bytes.Buffer) (*PubackPacket, error) {
	var packetID uint16
	if err := binary.Read(r, binary.BigEndian, &packetID); err != nil {
		return nil, err
	}

	return &PubackPacket{PacketID: packetID}, nil
}

func writeConnack(w io.Writer, pkt *ConnackPacket) error {
	var flags byte
	if pkt.SessionPresent {
		flags = 1
	}

	body := []byte{flags, pkt.ReturnCode}
	header := make([]byte, 0, 1+4)
	header = append(header, packetTypeCONNACK<<4)

	header = append(header, encodeRemainingLength(len(body))...)
	if _, err := w.Write(header); err != nil {
		return err
	}

	_, err := w.Write(body)

	return err
}

func writeSuback(w io.Writer, pkt *SubackPacket) error {
	body := &bytes.Buffer{}
	if err := binary.Write(body, binary.BigEndian, pkt.PacketID); err != nil {
		return err
	}

	body.Write(pkt.Granted)

	header := make([]byte, 0, 1+4)
	header = append(header, packetTypeSUBACK<<4)

	header = append(header, encodeRemainingLength(body.Len())...)
	if _, err := w.Write(header); err != nil {
		return err
	}

	_, err := w.Write(body.Bytes())

	return err
}

func writePublish(w io.Writer, pkt *PublishPacket) error {
	body := &bytes.Buffer{}
	if err := writeString(body, pkt.Topic); err != nil {
		return err
	}

	flags := byte(packetTypePUBLISH<<4) | (pkt.QoS << 1)
	if pkt.Duplicate {
		flags |= 0x08
	}

	if pkt.QoS > qos0 {
		if err := binary.Write(body, binary.BigEndian, pkt.PacketID); err != nil {
			return err
		}
	}

	body.Write(pkt.Payload)

	header := make([]byte, 0, 1+4)
	header = append(header, flags)

	header = append(header, encodeRemainingLength(body.Len())...)
	if _, err := w.Write(header); err != nil {
		return err
	}

	_, err := w.Write(body.Bytes())

	return err
}

func writePuback(w io.Writer, pkt *PubackPacket) error {
	body := &bytes.Buffer{}
	if err := binary.Write(body, binary.BigEndian, pkt.PacketID); err != nil {
		return err
	}

	header := make([]byte, 0, 1+4)
	header = append(header, packetTypePUBACK<<4)

	header = append(header, encodeRemainingLength(body.Len())...)
	if _, err := w.Write(header); err != nil {
		return err
	}

	_, err := w.Write(body.Bytes())

	return err
}

func writePingresp(w io.Writer, _ *PingrespPacket) error {
	header := []byte{packetTypePINGRESP << 4, 0}
	_, err := w.Write(header)

	return err
}

func toUint16Length(n int) (uint16, error) {
	if n < 0 || n > maxUint16 {
		return 0, fmt.Errorf("string too long")
	}

	return uint16(n), nil // #nosec G115 -- bounds checked above.
}
