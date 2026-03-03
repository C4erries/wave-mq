package mqtt

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
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
	addr    string
	broker  BrokerAPI
	options ServerOptions
	ln      net.Listener
}

type ServerOptions struct {
	EmptyPollInterval  time.Duration
	FetchErrorBackoff  time.Duration
	QoS1RetryInterval  time.Duration
	CommitRetryBackoff time.Duration
}

func defaultServerOptions() ServerOptions {
	return ServerOptions{
		EmptyPollInterval:  50 * time.Millisecond,
		FetchErrorBackoff:  100 * time.Millisecond,
		QoS1RetryInterval:  300 * time.Millisecond,
		CommitRetryBackoff: 100 * time.Millisecond,
	}
}

// NewServer constructs an MQTT server.
func NewServer(addr string, broker BrokerAPI) (*Server, error) {
	return NewServerWithOptions(addr, broker, defaultServerOptions())
}

// NewServerWithOptions constructs an MQTT server with explicit runtime options.
func NewServerWithOptions(addr string, broker BrokerAPI, options ServerOptions) (*Server, error) {
	if broker == nil {
		return nil, fmt.Errorf("broker is required")
	}

	if addr == "" {
		return nil, fmt.Errorf("addr is required")
	}

	if options.EmptyPollInterval <= 0 {
		options.EmptyPollInterval = defaultServerOptions().EmptyPollInterval
	}

	if options.FetchErrorBackoff <= 0 {
		options.FetchErrorBackoff = defaultServerOptions().FetchErrorBackoff
	}

	if options.QoS1RetryInterval <= 0 {
		options.QoS1RetryInterval = defaultServerOptions().QoS1RetryInterval
	}

	if options.CommitRetryBackoff <= 0 {
		options.CommitRetryBackoff = defaultServerOptions().CommitRetryBackoff
	}

	return &Server{addr: addr, broker: broker, options: options}, nil
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
		conn:             conn,
		ctx:              ctx,
		cancel:           cancel,
		subs:             make(map[string]subscriptionState),
		broker:           s.broker,
		opts:             s.options,
		outboundQoS1Acks: make(map[uint16]outboundQoS1),
		inboundQoS1Seen:  make(map[uint16]uint64),
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
		case *PubackPacket:
			state.ackOutgoing(v.PacketID)
			continue
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
	opts   ServerOptions

	mu   sync.Mutex
	subs map[string]subscriptionState // mqtt topic -> state

	nextPacketID     uint16
	outboundQoS1Acks map[uint16]outboundQoS1
	inboundQoS1Seen  map[uint16]uint64

	writeMu sync.Mutex
}

type subscriptionState struct {
	topic     string
	partition int
	qos       byte
	offset    api.Offset
	stop      context.CancelFunc
}

type outboundQoS1 struct {
	acked chan struct{}
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
			time.Sleep(state.opts.FetchErrorBackoff)

			continue
		}

		if len(recs) == 0 {
			time.Sleep(state.opts.EmptyPollInterval)
			continue
		}

		for _, r := range recs {
			pkt := &PublishPacket{
				Topic:   mqttTopic,
				QoS:     sub.qos,
				Payload: r.Value,
			}

			if sub.qos == qos1 {
				if err := state.sendQoS1AndWaitAck(ctx, pkt, sub, r.Offset); err != nil {
					return
				}
			} else {
				if err := state.writePublish(pkt); err != nil {
					return
				}

				if err := state.commitWithRetry(ctx, sub.topic, sub.partition, r.Offset); err != nil {
					return
				}
			}

			offset = r.Offset + 1
		}
	}
}

func (s *Server) handlePublish(state *clientState, pkt *PublishPacket) error {
	if pkt.QoS == qos1 && pkt.Duplicate && state.isDuplicateIncomingQoS1(pkt) {
		state.writeMu.Lock()
		defer state.writeMu.Unlock()

		return writePuback(state.conn, &PubackPacket{PacketID: pkt.PacketID})
	}

	internalTopic, partition, err := s.mapTopic(state.ctx, pkt.Topic, state.clientID)
	if err != nil {
		return err
	}

	record := api.Record{Value: pkt.Payload}
	if _, err := s.broker.Produce(state.ctx, internalTopic, partition, []api.Record{record}); err != nil {
		return err
	}

	if pkt.QoS == qos1 {
		state.markIncomingQoS1(pkt)

		state.writeMu.Lock()
		defer state.writeMu.Unlock()

		return writePuback(state.conn, &PubackPacket{PacketID: pkt.PacketID})
	}

	return nil
}

func (s *Server) handleDisconnect(state *clientState) {
	if state.group != "" && state.clientID != "" {
		_ = s.broker.LeaveGroup(state.ctx, state.group, state.clientID)
	}

	state.cancel()

	state.mu.Lock()
	defer state.mu.Unlock()

	for _, sub := range state.subs {
		if sub.stop != nil {
			sub.stop()
		}
	}
}

func (state *clientState) sendQoS1AndWaitAck(
	ctx context.Context,
	pkt *PublishPacket,
	sub subscriptionState,
	offset api.Offset,
) error {
	packetID, acked := state.registerOutgoing()
	pkt.PacketID = packetID
	pkt.Duplicate = false

	if err := state.writePublish(pkt); err != nil {
		state.cancelOutgoing(packetID)

		return err
	}

	retryTimer := time.NewTimer(state.opts.QoS1RetryInterval)
	defer retryTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			state.cancelOutgoing(packetID)

			return ctx.Err()
		case <-acked:
			return state.commitWithRetry(ctx, sub.topic, sub.partition, offset)
		case <-retryTimer.C:
			pkt.Duplicate = true
			if err := state.writePublish(pkt); err != nil {
				state.cancelOutgoing(packetID)

				return err
			}

			retryTimer.Reset(state.opts.QoS1RetryInterval)
		}
	}
}

func (state *clientState) writePublish(pkt *PublishPacket) error {
	state.writeMu.Lock()
	defer state.writeMu.Unlock()

	return writePublish(state.conn, pkt)
}

func (state *clientState) commitWithRetry(ctx context.Context, topic string, partition int, offset api.Offset) error {
	for {
		if err := state.broker.CommitOffset(ctx, state.group, topic, partition, offset); err != nil {
			observability.RequestErrors.WithLabelValues("mqtt", "commit").Inc()
			slog.Error("mqtt commit failed", "topic", topic, "partition", partition, "offset", offset, "err", err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(state.opts.CommitRetryBackoff):
			}

			continue
		}

		return nil
	}
}

func (state *clientState) registerOutgoing() (uint16, <-chan struct{}) {
	state.mu.Lock()
	defer state.mu.Unlock()

	for {
		state.nextPacketID++
		if state.nextPacketID == 0 {
			continue
		}

		if _, exists := state.outboundQoS1Acks[state.nextPacketID]; exists {
			continue
		}

		ackCh := make(chan struct{})
		state.outboundQoS1Acks[state.nextPacketID] = outboundQoS1{
			acked: ackCh,
		}

		return state.nextPacketID, ackCh
	}
}

func (state *clientState) ackOutgoing(packetID uint16) {
	state.mu.Lock()

	pending, ok := state.outboundQoS1Acks[packetID]

	if ok {
		delete(state.outboundQoS1Acks, packetID)
	}

	state.mu.Unlock()

	if ok {
		close(pending.acked)
	}
}

func (state *clientState) cancelOutgoing(packetID uint16) {
	state.mu.Lock()
	delete(state.outboundQoS1Acks, packetID)
	state.mu.Unlock()
}

func (state *clientState) isDuplicateIncomingQoS1(pkt *PublishPacket) bool {
	state.mu.Lock()
	defer state.mu.Unlock()

	fp, ok := state.inboundQoS1Seen[pkt.PacketID]

	return ok && fp == publishFingerprint(pkt.Topic, pkt.Payload)
}

func (state *clientState) markIncomingQoS1(pkt *PublishPacket) {
	state.mu.Lock()
	defer state.mu.Unlock()

	state.inboundQoS1Seen[pkt.PacketID] = publishFingerprint(pkt.Topic, pkt.Payload)
}

func publishFingerprint(topic string, payload []byte) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(topic))
	_, _ = hash.Write(payload)

	return hash.Sum64()
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
	_, _ = hash.Write([]byte(mqttTopic))
	_, _ = hash.Write([]byte(clientID))
	pid := int(hash.Sum32()) % len(partitions)

	return mqttTopic, partitions[pid], nil
}
