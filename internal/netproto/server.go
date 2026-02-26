package netproto

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/c4erries/wave-mq/internal/broker"
	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/pkg/api"
)

// BrokerAPI defines how the network layer talks to the broker core.
type BrokerAPI interface {
	CreateTopic(ctx context.Context, name string, cfg api.TopicConfig) error
	Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error)
	Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error)
	ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error)
	CommitOffset(ctx context.Context, group, topic string, partition int, offset api.Offset) error
	FetchCommitted(ctx context.Context, group, topic string, partition int) (api.Offset, error)
	Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error)
}

// Server hosts the custom binary protocol over TCP.
type Server struct {
	mu      sync.RWMutex
	addr    string
	broker  BrokerAPI
	ln      net.Listener
	started bool
}

// NewServer constructs a TCP server bound to addr.
func NewServer(addr string, brokerAPI BrokerAPI) (*Server, error) {
	if brokerAPI == nil {
		return nil, fmt.Errorf("broker is required")
	}

	if addr == "" {
		return nil, fmt.Errorf("addr is required")
	}

	return &Server{addr: addr, broker: brokerAPI}, nil
}

// ListenAndServe starts accepting client connections until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}

	s.started = true
	s.mu.Unlock()

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()

		return err
	}

	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.ln = nil
		s.started = false
		s.mu.Unlock()
	}()

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
	s.mu.RLock()
	ln := s.ln
	s.mu.RUnlock()

	if ln != nil {
		return ln.Close()
	}

	return nil
}

// Addr returns the listener address after ListenAndServe has started.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	ln := s.ln
	s.mu.RUnlock()

	if ln == nil {
		return nil
	}

	addr := ln.Addr()

	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return addr
	}

	copied := *tcp
	if tcp.IP != nil {
		copied.IP = append(net.IP(nil), tcp.IP...)
	}

	return &copied
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		apiKey, corr, payload, err := decodeRequestFrame(conn)
		if err != nil {
			if err == io.EOF {
				return
			}

			observability.RequestErrors.WithLabelValues("netproto", "decode_frame").Inc()

			return
		}

		respPayload, err := s.dispatch(conn, apiKey, payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "encode_response").Inc()
			return
		}

		frame, err := encodeResponseFrame(apiKey, corr, respPayload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "encode_response").Inc()
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
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		err = s.broker.CreateTopic(ctx, req.Topic, api.TopicConfig{Partitions: req.Partitions, ReplicationFactor: req.ReplicationFactor})

		resp := &CreateTopicResponse{}
		if err != nil {
			resp.Error = mapError(err)

			observability.RequestErrors.WithLabelValues("netproto", "broker_call").Inc()
		}

		return encodeCreateTopicResponse(resp)
	case api.APIKeyProduce:
		req, err := decodeProduceRequest(payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		base, err := s.broker.Produce(ctx, req.Topic, req.Partition, req.Records)

		resp := &ProduceResponse{BaseOffset: base}
		if err != nil {
			resp.Error = mapError(err)

			observability.RequestErrors.WithLabelValues("netproto", "broker_call").Inc()
		}

		return encodeProduceResponse(resp)
	case api.APIKeyFetch:
		req, err := decodeFetchRequest(payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		recs, err := s.broker.Fetch(ctx, req.Topic, req.Partition, req.Offset, req.MaxBytes)

		resp := &FetchResponse{Records: recs}
		if err != nil {
			resp.Error = mapError(err)

			observability.RequestErrors.WithLabelValues("netproto", "broker_call").Inc()
		} else {
			_, latest, offErr := s.broker.ListOffsets(ctx, req.Topic, req.Partition)
			if offErr == nil {
				resp.HighWatermark = latest
			}
		}

		return encodeFetchResponse(resp)
	case api.APIKeyMetadata:
		req, err := decodeMetadataRequest(payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		md, err := s.broker.Metadata(ctx, req.Topics)

		resp := &MetadataResponse{Partitions: md}
		if err != nil {
			resp.Error = mapError(err)

			observability.RequestErrors.WithLabelValues("netproto", "broker_call").Inc()
		}

		return encodeMetadataResponse(resp)
	case api.APIKeyPing:
		req, err := decodePingRequest(payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		_ = req

		return encodePingResponse(&PingResponse{Error: api.ErrNone})
	case api.APIKeyCommitOffset:
		req, err := decodeCommitOffsetRequest(payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		resp := &CommitOffsetResponse{}
		if err := s.broker.CommitOffset(ctx, req.Group, req.Topic, req.Partition, req.Offset); err != nil {
			resp.Error = mapError(err)

			observability.RequestErrors.WithLabelValues("netproto", "broker_call").Inc()
		}

		return encodeCommitOffsetResponse(resp)
	case api.APIKeyFetchCommitted:
		req, err := decodeFetchCommittedRequest(payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		offset, err := s.broker.FetchCommitted(ctx, req.Group, req.Topic, req.Partition)

		resp := &FetchCommittedResponse{Offset: offset}
		if err != nil {
			resp.Error = mapError(err)

			observability.RequestErrors.WithLabelValues("netproto", "broker_call").Inc()
		}

		return encodeFetchCommittedResponse(resp)
	case api.APIKeyListOffsets:
		req, err := decodeListOffsetsRequest(payload)
		if err != nil {
			observability.RequestErrors.WithLabelValues("netproto", "decode_request").Inc()
			return s.errorResponseForKey(apiKey, api.ErrInvalidRequest)
		}

		earliest, latest, err := s.broker.ListOffsets(ctx, req.Topic, req.Partition)

		resp := &ListOffsetsResponse{Earliest: earliest, Latest: latest}
		if err != nil {
			resp.Error = mapError(err)

			observability.RequestErrors.WithLabelValues("netproto", "broker_call").Inc()
		}

		return encodeListOffsetsResponse(resp)
	default:
		return nil, fmt.Errorf("unknown api key %d", apiKey)
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

func mapError(err error) api.ErrorCode {
	switch {
	case err == nil:
		return api.ErrNone
	case errors.Is(err, broker.ErrTopicNotFound):
		return api.ErrTopicNotFound
	case errors.Is(err, broker.ErrPartitionNotFound):
		return api.ErrPartitionNotFound
	case errors.Is(err, broker.ErrTopicExists):
		return api.ErrTopicExists
	case errors.Is(err, broker.ErrNotLeader):
		return api.ErrNotLeader
	case errors.As(err, &broker.NotLeaderError{}):
		return api.ErrNotLeader
	default:
		return api.ErrInternal
	}
}
