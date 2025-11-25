package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/c4erries/wave-mq/pkg/api"
)

type commandType uint8

const (
	cmdAssignTopic commandType = iota + 1
	cmdReportReplicaProgress
	cmdRegisterBroker
)

type raftCommand struct {
	Type commandType

	Topic string
	Part  int

	Broker int

	TopicConfig api.TopicConfig

	LastOffset          api.Offset
	LeaderHighWatermark api.Offset

	BrokerInfo *api.BrokerInfo
}

func encodeCommand(cmd raftCommand) ([]byte, error) {
	return json.Marshal(cmd)
}

func decodeCommand(b []byte) (raftCommand, error) {
	var cmd raftCommand
	err := json.Unmarshal(b, &cmd)
	return cmd, err
}

type raftMetadataFSM struct {
	mu   sync.Mutex
	meta api.ClusterMetadata
	cfg  api.BrokerConfig
}

func newRaftMetadataFSM(cfg api.BrokerConfig, initial api.ClusterMetadata) *raftMetadataFSM {
	return &raftMetadataFSM{meta: initial, cfg: cfg}
}

func (f *raftMetadataFSM) Apply(l *raft.Log) interface{} {
	cmd, err := decodeCommand(l.Data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Type {
	case cmdAssignTopic:
		newParts := assignTopicPartitions(f.cfg, f.meta.Brokers, cmd.Topic, cmd.TopicConfig.Partitions, f.meta.Partitions)
		f.meta.Partitions = append(f.meta.Partitions, newParts...)
		f.meta.Version++
	case cmdReportReplicaProgress:
		err = f.applyReplicaProgressLocked(cmd.Topic, cmd.Part, cmd.Broker, cmd.LastOffset, cmd.LeaderHighWatermark)
	case cmdRegisterBroker:
		if cmd.BrokerInfo == nil {
			err = fmt.Errorf("missing broker info")
		} else {
			err = f.applyRegisterBrokerLocked(*cmd.BrokerInfo)
		}
	default:
		err = fmt.Errorf("unknown command type %d", cmd.Type)
	}
	return err
}

func (f *raftMetadataFSM) applyReplicaProgressLocked(topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) error {
	idx := -1
	for i, p := range f.meta.Partitions {
		if p.Topic == topic && p.Partition == partition {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("partition not found")
	}
	assign := f.meta.Partitions[idx]
	if !contains(assign.Replicas, brokerID) {
		return fmt.Errorf("broker %d not in replicas", brokerID)
	}
	shouldBeISR := lastOffset >= leaderHighWatermark
	assign.ISR = ensureLeaderInISR(assign.Leader, assign.ISR)
	inISR := contains(assign.ISR, brokerID)
	switch {
	case shouldBeISR && !inISR:
		assign.ISR = append(assign.ISR, brokerID)
	case !shouldBeISR && inISR && brokerID != assign.Leader:
		assign.ISR = remove(assign.ISR, brokerID)
	}
	f.meta.Partitions[idx] = assign
	f.meta.Version++
	return nil
}

func (f *raftMetadataFSM) applyRegisterBrokerLocked(info api.BrokerInfo) error {
	updated := false
	for i, b := range f.meta.Brokers {
		if b.BrokerID == info.BrokerID {
			if b.Host == info.Host && b.Port == info.Port && b.Rack == info.Rack {
				return nil
			}
			f.meta.Brokers[i] = info
			updated = true
			break
		}
	}
	if !updated {
		f.meta.Brokers = append(f.meta.Brokers, info)
	}
	sort.Slice(f.meta.Brokers, func(i, j int) bool { return f.meta.Brokers[i].BrokerID < f.meta.Brokers[j].BrokerID })
	f.meta.Version++
	return nil
}

func (f *raftMetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := json.Marshal(f.meta)
	if err != nil {
		return nil, err
	}
	return &metadataSnapshot{data: b}, nil
}

func (f *raftMetadataFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var meta api.ClusterMetadata
	if err := json.NewDecoder(r).Decode(&meta); err != nil {
		return err
	}
	f.mu.Lock()
	f.meta = meta
	f.mu.Unlock()
	return nil
}

type metadataSnapshot struct {
	data []byte
}

func (s *metadataSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *metadataSnapshot) Release() {}

// RaftController replicates cluster metadata through a Raft log.
type RaftController struct {
	cfg  api.BrokerConfig
	fsm  *raftMetadataFSM
	raft *raft.Raft
}

// NewRaftController bootstraps a Raft instance with given initial metadata.
func NewRaftController(cfg api.BrokerConfig, initialMeta api.ClusterMetadata, raftDir string) (*RaftController, error) {
	rCfg := raft.DefaultConfig()
	useInmem := cfg.RaftBindAddr == "" || len(cfg.RaftPeers) == 0
	var localAddr raft.ServerAddress
	var localID raft.ServerID

	if useInmem {
		localID = raft.ServerID(fmt.Sprintf("broker-%d", cfg.BrokerID))
	} else {
		localAddr = raft.ServerAddress(cfg.RaftBindAddr)
		localID = raft.ServerID(localAddr)
	}
	rCfg.LocalID = localID
	rCfg.SnapshotInterval = 2 * time.Second
	rCfg.SnapshotThreshold = 64
	rCfg.HeartbeatTimeout = 50 * time.Millisecond
	rCfg.ElectionTimeout = 50 * time.Millisecond
	rCfg.LeaderLeaseTimeout = 50 * time.Millisecond
	rCfg.CommitTimeout = 10 * time.Millisecond

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewInmemSnapshotStore()
	var transport raft.Transport
	if useInmem {
		addr, inmem := raft.NewInmemTransport(raft.ServerAddress(rCfg.LocalID))
		transport = inmem
		localAddr = addr
	} else {
		tcpTransport, err := raft.NewTCPTransport(cfg.RaftBindAddr, nil, 3, 2*time.Second, io.Discard)
		if err != nil {
			return nil, err
		}
		transport = tcpTransport
		localAddr = raft.ServerAddress(cfg.RaftBindAddr)
	}

	if len(cfg.RaftPeers) == 0 {
		cfg.RaftPeers = []string{string(localAddr)}
	}
	config := raft.Configuration{
		Servers: buildServers(cfg, rCfg.LocalID, localAddr),
	}
	if err := raft.BootstrapCluster(rCfg, logStore, stableStore, snapStore, transport, config); err != nil && err != raft.ErrCantBootstrap {
		return nil, err
	}

	fsm := newRaftMetadataFSM(cfg, initialMeta)
	r, err := raft.NewRaft(rCfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, err
	}
	return &RaftController{cfg: cfg, fsm: fsm, raft: r}, nil
}

func (c *RaftController) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx
	c.fsm.mu.Lock()
	defer c.fsm.mu.Unlock()
	return c.fsm.meta, nil
}

func (c *RaftController) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = sinceVersion
	ch := make(chan api.ClusterMetadata, 1)
	go func() {
		defer close(ch)
		meta, _ := c.GetClusterMetadata(ctx)
		select {
		case <-ctx.Done():
		case ch <- meta:
		}
	}()
	return ch, nil
}

func (c *RaftController) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	cmd := raftCommand{
		Type:       cmdRegisterBroker,
		BrokerInfo: &info,
	}
	return c.applyCommand(ctx, cmd)
}

func (c *RaftController) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	cmd := raftCommand{
		Type:        cmdAssignTopic,
		Topic:       name,
		TopicConfig: cfg,
	}
	if err := c.applyCommand(ctx, cmd); err != nil {
		return api.ClusterMetadata{}, err
	}
	return c.GetClusterMetadata(ctx)
}

func (c *RaftController) ReportReplicaProgress(ctx context.Context, topic string, partition int, brokerID int, lastOffset api.Offset, leaderHighWatermark api.Offset) (api.ClusterMetadata, error) {
	cmd := raftCommand{
		Type:                cmdReportReplicaProgress,
		Topic:               topic,
		Part:                partition,
		Broker:              brokerID,
		LastOffset:          lastOffset,
		LeaderHighWatermark: leaderHighWatermark,
	}
	if err := c.applyCommand(ctx, cmd); err != nil {
		return api.ClusterMetadata{}, err
	}
	return c.GetClusterMetadata(ctx)
}

func (c *RaftController) applyCommand(ctx context.Context, cmd raftCommand) error {
	if err := c.waitForLeader(ctx); err != nil {
		return err
	}
	data, err := encodeCommand(cmd)
	if err != nil {
		return err
	}
	future := c.raft.Apply(data, time.Second*5)
	done := make(chan error, 1)
	go func() {
		done <- future.Error()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return err
		}
		if applyErr, ok := future.Response().(error); ok && applyErr != nil {
			return applyErr
		}
		return nil
	}
}

func (c *RaftController) waitForLeader(ctx context.Context) error {
	if c.raft.State() == raft.Leader {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("leader not elected")
		case <-ticker.C:
			if c.raft.State() == raft.Leader {
				return nil
			}
		}
	}
}

// SnapshotDump returns the encoded metadata snapshot (for tests).
func (c *RaftController) SnapshotDump() ([]byte, error) {
	c.fsm.mu.Lock()
	defer c.fsm.mu.Unlock()
	return json.Marshal(c.fsm.meta)
}

// ControllerMode returns the configured controller mode.
func (c *RaftController) ControllerMode() string { return "raft" }

// RaftState returns the current raft state as string.
func (c *RaftController) RaftState() string { return c.raft.State().String() }

// RaftTerm returns the current term.
func (c *RaftController) RaftTerm() uint64 {
	stats := c.raft.Stats()
	if termStr, ok := stats["term"]; ok {
		var term uint64
		fmt.Sscanf(termStr, "%d", &term)
		return term
	}
	return 0
}

// RaftPeers returns peer addresses.
func (c *RaftController) RaftPeers() []string {
	servers := c.raft.GetConfiguration().Configuration().Servers
	out := make([]string, 0, len(servers))
	for _, s := range servers {
		out = append(out, string(s.Address))
	}
	return out
}

// Close stops the underlying Raft instance.
func (c *RaftController) Close() error {
	f := c.raft.Shutdown()
	return f.Error()
}

func buildServers(cfg api.BrokerConfig, localID raft.ServerID, localAddr raft.ServerAddress) []raft.Server {
	seen := make(map[raft.ServerID]struct{})
	var servers []raft.Server
	add := func(id raft.ServerID, addr raft.ServerAddress) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: id, Address: addr})
	}
	add(localID, localAddr)
	for _, peer := range cfg.RaftPeers {
		id := raft.ServerID(peer)
		addr := raft.ServerAddress(peer)
		add(id, addr)
	}
	return servers
}
