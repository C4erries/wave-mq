package mqtt

// The packet types below are intentionally minimal. They provide a placeholder
// for MQTT 3.1.1/5.0 fields that will be parsed/serialized later.

type ConnectPacket struct {
	ClientID     string
	KeepAliveSec uint16
	CleanStart   bool
	Username     string
	AuthData     []byte
}

type ConnackPacket struct {
	SessionPresent bool
	ReturnCode     byte
}

type SubscribePacket struct {
	PacketID uint16
	Topics   []Subscription
}

type Subscription struct {
	Filter string
	QoS    byte
}

type SubackPacket struct {
	PacketID uint16
	Granted  []byte
}

type PublishPacket struct {
	Topic    string
	QoS      byte
	PacketID uint16
	Payload  []byte
}

type PubackPacket struct {
	PacketID uint16
}

type PingreqPacket struct{}

type PingrespPacket struct{}

type DisconnectPacket struct{}
