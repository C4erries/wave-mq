package netproto

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"

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
	corrID  int32
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
	for {
		apiKey, corr, payload, err := decodeRequestFrame(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		respPayload, err := s.dispatch(conn, apiKey, payload)
		if err != nil {
			return
		}
		frame, err := encodeResponseFrame(apiKey, corr, respPayload)
		if err != nil {
			return
		}
		if _, err := conn.Write(frame); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(conn net.Conn, apiKey api.APIKey, payload []byte) ([]byte, error) {
	ctx := context.Background()
	switch apiKey {
	case api.APIKeyCreateTopic:
		req, err := decodeCreateTopicRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		err = s.broker.CreateTopic(ctx, req.Topic, api.TopicConfig{Partitions: req.Partitions, ReplicationFactor: req.ReplicationFactor})
		resp := &CreateTopicResponse{}
		if err != nil {
			resp.Error = mapError(err)
		}
		return encodeCreateTopicResponse(resp)
	case api.APIKeyProduce:
		req, err := decodeProduceRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		base, err := s.broker.Produce(ctx, req.Topic, req.Partition, req.Records)
		resp := &ProduceResponse{BaseOffset: base}
		if err != nil {
			resp.Error = mapError(err)
		}
		return encodeProduceResponse(resp)
	case api.APIKeyFetch:
		req, err := decodeFetchRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		recs, err := s.broker.Fetch(ctx, req.Topic, req.Partition, req.Offset, req.MaxBytes)
		resp := &FetchResponse{Records: recs}
		if err != nil {
			resp.Error = mapError(err)
		}
		return encodeFetchResponse(resp)
	case api.APIKeyMetadata:
		req, err := decodeMetadataRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		md, err := s.broker.Metadata(ctx, req.Topics)
		resp := &MetadataResponse{Partitions: md}
		if err != nil {
			resp.Error = mapError(err)
		}
		return encodeMetadataResponse(resp)
	case api.APIKeyPing:
		req, err := decodePingRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		_ = req
		return encodePingResponse(&PingResponse{Error: api.ErrNone})
	case api.APIKeyCommitOffset:
		req, err := decodeCommitOffsetRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		resp := &CommitOffsetResponse{}
		if err := s.broker.CommitOffset(ctx, req.Group, req.Topic, req.Partition, req.Offset); err != nil {
			resp.Error = mapError(err)
		}
		return encodeCommitOffsetResponse(resp)
	case api.APIKeyFetchCommitted:
		req, err := decodeFetchCommittedRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		offset, err := s.broker.FetchCommitted(ctx, req.Group, req.Topic, req.Partition)
		resp := &FetchCommittedResponse{Offset: offset}
		if err != nil {
			resp.Error = mapError(err)
		}
		return encodeFetchCommittedResponse(resp)
	case api.APIKeyListOffsets:
		req, err := decodeListOffsetsRequest(payload)
		if err != nil {
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}
		earliest, latest, err := s.broker.ListOffsets(ctx, req.Topic, req.Partition)
		resp := &ListOffsetsResponse{Earliest: earliest, Latest: latest}
		if err != nil {
			resp.Error = mapError(err)
		}
		return encodeListOffsetsResponse(resp)
	default:
		return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
	}
}

func (s *Server) errorResponseForKey(apiKey api.APIKey, code api.ErrorCode) ([]byte, error) {
	switch apiKey {
	case api.APIKeyCreateTopic:
		return encodeCreateTopicResponse(&CreateTopicResponse{Error: code})
	case api.APIKeyProduce:
		return encodeProduceResponse(&ProduceResponse{BaseOffset: -1, Error: code})
	case api.APIKeyFetch:
		return encodeFetchResponse(&FetchResponse{Records: nil, Error: code})
	case api.APIKeyMetadata:
		return encodeMetadataResponse(&MetadataResponse{Error: code})
	case api.APIKeyPing:
		return encodePingResponse(&PingResponse{Error: code})
	case api.APIKeyCommitOffset:
		return encodeCommitOffsetResponse(&CommitOffsetResponse{Error: code})
	case api.APIKeyFetchCommitted:
		return encodeFetchCommittedResponse(&FetchCommittedResponse{Error: code})
	case api.APIKeyListOffsets:
		return encodeListOffsetsResponse(&ListOffsetsResponse{Error: code})
	default:
		return encodePingResponse(&PingResponse{Error: code})
	}
}

func (s *Server) nextCorrID() int32 {
	return atomic.AddInt32(&s.corrID, 1)
}

func mapError(err error) api.ErrorCode {
	if err == nil {
		return api.ErrNone
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "topic not found"):
		return api.ErrTopicNotFound
	case strings.Contains(msg, "partition not found"):
		return api.ErrPartitionNotFound
	case strings.Contains(msg, "broker closed"):
		return api.ErrInternal
	default:
		return api.ErrInternal
	}
}
