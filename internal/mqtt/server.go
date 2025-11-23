package mqtt

import (
	"context"
	"fmt"
	"net"

	"github.com/c4erries/wave-mq/pkg/api"
)

// BrokerAPI is the minimal interface the MQTT frontend needs.
type BrokerAPI interface {
	Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error)
	Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error)
}

// Server hosts the MQTT TCP listener and packet loop.
type Server struct {
	addr   string
	broker BrokerAPI
	ln     net.Listener
}

// NewServer constructs an MQTT server.
func NewServer(addr string, broker BrokerAPI) (*Server, error) {
	if broker == nil {
		return nil, fmt.Errorf("broker is required")
	}
	if addr == "" {
		return nil, fmt.Errorf("addr is required")
	}
	return &Server{addr: addr, broker: broker}, nil
}

// ListenAndServe accepts MQTT clients until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			return err
		}
		go s.handleConnection(conn)
	}
}

// Close stops the listener.
func (s *Server) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	// TODO: parse MQTT packets and dispatch to handlers.
}

// handleConnect processes an MQTT CONNECT packet.
func (s *Server) handleConnect(pkt ConnectPacket) (*ConnackPacket, error) {
	_ = pkt
	panic("not implemented")
}

// handleSubscribe processes SUBSCRIBE packets.
func (s *Server) handleSubscribe(pkt SubscribePacket) (*SubackPacket, error) {
	_ = pkt
	panic("not implemented")
}

// handlePublish processes PUBLISH packets (QoS0/1).
func (s *Server) handlePublish(pkt PublishPacket) (*PubackPacket, error) {
	_ = pkt
	panic("not implemented")
}

// handlePingReq processes PINGREQ packets.
func (s *Server) handlePingReq(pkt PingreqPacket) (*PingrespPacket, error) {
	_ = pkt
	panic("not implemented")
}

// handleDisconnect processes DISCONNECT packets.
func (s *Server) handleDisconnect(pkt DisconnectPacket) error {
	_ = pkt
	panic("not implemented")
}
