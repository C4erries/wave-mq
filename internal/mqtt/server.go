package mqtt

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/c4erries/wave-mq/internal/observability"
	"github.com/c4erries/wave-mq/pkg/api"
)

// BrokerAPI is the minimal interface the MQTT frontend needs.
type BrokerAPI interface {
	Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error)
	Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error)
	ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error)
	Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error)
	JoinGroup(ctx context.Context, group, memberID string, topics []string) (map[string][]int, error)
	LeaveGroup(ctx context.Context, group, memberID string) error
	CommitOffset(ctx context.Context, group, topic string, partition int, offset api.Offset) error
	FetchCommitted(ctx context.Context, group, topic string, partition int) (api.Offset, error)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := &clientState{
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		subs:   make(map[string]subscriptionState),
		broker: s.broker,
	}
	for {
		pkt, err := readPacket(conn)
		if err != nil {
			if err != io.EOF {
				observability.RequestErrors.WithLabelValues("mqtt", "decode_packet").Inc()
			}
			return
		}
		switch v := pkt.(type) {
		case *ConnectPacket:
			if err := s.handleConnect(state, v); err != nil {
				observability.RequestErrors.WithLabelValues("mqtt", "connect").Inc()
				return
			}
		case *SubscribePacket:
			if err := s.handleSubscribe(state, v); err != nil {
				observability.RequestErrors.WithLabelValues("mqtt", "subscribe").Inc()
				return
			}
		case *PublishPacket:
			if err := s.handlePublish(state, v); err != nil {
				observability.RequestErrors.WithLabelValues("mqtt", "publish").Inc()
				return
			}
		case *PingreqPacket:
			state.writeMu.Lock()
			_ = writePingresp(conn, &PingrespPacket{})
			state.writeMu.Unlock()
		case *DisconnectPacket:
			s.handleDisconnect(state)
			return
		default:
			return
		}
	}
}

type clientState struct {
	conn   net.Conn
	ctx    context.Context
	cancel context.CancelFunc

	clientID   string
	cleanStart bool
	keepAlive  time.Duration
	group      string

	broker BrokerAPI

	mu   sync.Mutex
	subs map[string]subscriptionState // mqtt topic -> state

	writeMu sync.Mutex
}

type subscriptionState struct {
	topic     string
	partition int
	qos       byte
	offset    api.Offset
	stop      context.CancelFunc
}

func (s *Server) handleConnect(state *clientState, pkt *ConnectPacket) error {
	if pkt.ClientID == "" {
		return fmt.Errorf("client id required")
	}
	state.clientID = pkt.ClientID
	state.group = pkt.ClientID // group == clientID for MQTT clients
	state.cleanStart = pkt.CleanStart
	state.keepAlive = time.Duration(pkt.KeepAliveSec) * time.Second
	resp := &ConnackPacket{SessionPresent: false, ReturnCode: 0}
	state.writeMu.Lock()
	defer state.writeMu.Unlock()
	return writeConnack(state.conn, resp)
}

func (s *Server) handleSubscribe(state *clientState, pkt *SubscribePacket) error {
	ack := &SubackPacket{PacketID: pkt.PacketID}
	granted := make([]byte, len(pkt.Topics))
	for i, sub := range pkt.Topics {
		if sub.QoS > qos1 {
			granted[i] = 0x80
			continue
		}
		assignments, err := s.broker.JoinGroup(state.ctx, state.group, state.clientID, []string{sub.Filter})
		if err != nil {
			granted[i] = 0x80
			continue
		}
		partitions := assignments[sub.Filter]
		if len(partitions) == 0 {
			granted[i] = 0x80
			continue
		}
		granted[i] = sub.QoS
		for _, p := range partitions {
			state.trackSubscription(sub.Filter, sub.Filter, p, sub.QoS)
		}
	}
	ack.Granted = granted
	state.writeMu.Lock()
	defer state.writeMu.Unlock()
	if err := writeSuback(state.conn, ack); err != nil {
		return err
	}
	return nil
}

func (state *clientState) trackSubscription(mqttTopic, topic string, partition int, qos byte) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if old, ok := state.subs[mqttTopic]; ok && old.stop != nil {
		old.stop()
	}
	ctx, cancel := context.WithCancel(state.ctx)
	sub := subscriptionState{
		topic:     topic,
		partition: partition,
		qos:       qos,
		stop:      cancel,
	}
	sub.offset = state.initialOffsetForSub(ctx, topic, partition)
	state.subs[mqttTopic] = sub
	go state.consumeLoop(ctx, mqttTopic, sub)
}

func (state *clientState) initialOffsetForSub(ctx context.Context, topic string, partition int) api.Offset {
	// Tail-only: ignore committed offsets, start at latest+1.
	if state.cleanStart {
		_, latest, err := state.broker.ListOffsets(ctx, topic, partition)
		if err != nil {
			return 0
		}
		return latest + 1
	}
	// Resume mode: start from max(earliest, committed+1).
	earliest, _, errEarliest := state.broker.ListOffsets(ctx, topic, partition)
	committed, errCommitted := state.broker.FetchCommitted(ctx, state.group, topic, partition)
	start := earliest
	if errCommitted == nil && committed+1 > start {
		start = committed + 1
	}
	if errEarliest != nil {
		return 0
	}
	return start
}

func (state *clientState) consumeLoop(ctx context.Context, mqttTopic string, sub subscriptionState) {
	offset := sub.offset
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		recs, err := state.broker.Fetch(ctx, sub.topic, sub.partition, offset, 64<<10)
		if err != nil {
			observability.RequestErrors.WithLabelValues("mqtt", "fetch").Inc()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(recs) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, r := range recs {
			pkt := &PublishPacket{
				Topic:   mqttTopic,
				QoS:     sub.qos,
				Payload: r.Value,
			}
			state.writeMu.Lock()
			_ = writePublish(state.conn, pkt)
			state.writeMu.Unlock()
			// Commit offset after sending to client.
			_ = state.broker.CommitOffset(ctx, state.group, sub.topic, sub.partition, r.Offset)
			offset = r.Offset + 1
		}
	}
}

func (s *Server) handlePublish(state *clientState, pkt *PublishPacket) error {
	internalTopic, partition, err := s.mapTopic(state.ctx, pkt.Topic, state.clientID)
	if err != nil {
		return err
	}
	record := api.Record{Value: pkt.Payload}
	if _, err := s.broker.Produce(state.ctx, internalTopic, partition, []api.Record{record}); err != nil {
		return err
	}
	if pkt.QoS == qos1 {
		state.writeMu.Lock()
		defer state.writeMu.Unlock()
		return writePuback(state.conn, &PubackPacket{PacketID: pkt.PacketID})
	}
	return nil
}

func (s *Server) handleDisconnect(state *clientState) {
	if state.group != "" && state.clientID != "" {
		_ = s.broker.LeaveGroup(context.Background(), state.group, state.clientID)
	}
	state.cancel()
	for _, sub := range state.subs {
		if sub.stop != nil {
			sub.stop()
		}
	}
}

func (s *Server) mapTopic(ctx context.Context, mqttTopic, clientID string) (string, int, error) {
	meta, err := s.broker.Metadata(ctx, []string{mqttTopic})
	if err != nil {
		return "", 0, err
	}
	var partitions []int
	for _, m := range meta {
		if m.Replica.Topic == mqttTopic {
			partitions = append(partitions, m.Replica.Partition)
		}
	}
	if len(partitions) == 0 {
		return "", 0, fmt.Errorf("topic not found")
	}
	sort.Ints(partitions)
	hash := fnv.New32a()
	hash.Write([]byte(mqttTopic))
	hash.Write([]byte(clientID))
	pid := int(hash.Sum32()) % len(partitions)
	return mqttTopic, partitions[pid], nil
}
