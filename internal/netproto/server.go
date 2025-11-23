package netproto

import (
	"context"
	"fmt"
	"net"

	"github.com/c4erries/wave-mq/pkg/api"
)

// BrokerAPI defines how the network layer talks to the broker core.
type BrokerAPI interface {
	CreateTopic(ctx context.Context, name string, cfg api.TopicConfig) error
	Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error)
	Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error)
	ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error)
	CommitOffset(ctx context.Context, group string, topic string, partition int, offset api.Offset) error
	FetchCommitted(ctx context.Context, group string, topic string, partition int) (api.Offset, error)
	Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error)
}

// Server hosts the custom binary protocol over TCP.
type Server struct {
	addr    string
	broker  BrokerAPI
	ln      net.Listener
	started bool
}

// NewServer constructs a TCP server bound to addr.
func NewServer(addr string, broker BrokerAPI) (*Server, error) {
	if broker == nil {
		return nil, fmt.Errorf("broker is required")
	}
	if addr == "" {
		return nil, fmt.Errorf("addr is required")
	}
	return &Server{addr: addr, broker: broker}, nil
}

// ListenAndServe starts accepting client connections until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.started {
		return fmt.Errorf("server already started")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.started = true

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

// Close stops accepting new connections.
func (s *Server) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

// frame is a placeholder for length-prefixed protocol frames.
type frame struct {
	APIKey api.APIKey
	// TODO: add version, correlation ID, flags and payload buffer.
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	// TODO: parse length-prefixed frames and route to handlers.
}

// handleCreateTopic dispatches CreateTopic requests.
func (s *Server) handleCreateTopic(ctx context.Context, req CreateTopicRequest) (*CreateTopicResponse, error) {
	_ = ctx
	_ = req
	panic("not implemented")
}

// handleProduce dispatches Produce requests.
func (s *Server) handleProduce(ctx context.Context, req ProduceRequest) (*ProduceResponse, error) {
	_ = ctx
	_ = req
	panic("not implemented")
}

// handleFetch dispatches Fetch requests.
func (s *Server) handleFetch(ctx context.Context, req FetchRequest) (*FetchResponse, error) {
	_ = ctx
	_ = req
	panic("not implemented")
}

// handleMetadata dispatches Metadata requests.
func (s *Server) handleMetadata(ctx context.Context, req MetadataRequest) (*MetadataResponse, error) {
	_ = ctx
	_ = req
	panic("not implemented")
}

// handleCommitOffset dispatches CommitOffset requests.
func (s *Server) handleCommitOffset(ctx context.Context, req CommitOffsetRequest) (*CommitOffsetResponse, error) {
	_ = ctx
	_ = req
	panic("not implemented")
}

// handleFetchCommitted dispatches FetchCommitted requests.
func (s *Server) handleFetchCommitted(ctx context.Context, req FetchCommittedRequest) (*FetchCommittedResponse, error) {
	_ = ctx
	_ = req
	panic("not implemented")
}

// handlePing handles ping health checks.
func (s *Server) handlePing(ctx context.Context, req PingRequest) (*PingResponse, error) {
	_ = ctx
	_ = req
	panic("not implemented")
}
