package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

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
	pub  *metadataPublisher
}

func newRaftMetadataFSM(cfg api.BrokerConfig, initial api.ClusterMetadata, pub *metadataPublisher) *raftMetadataFSM {
	return &raftMetadataFSM{meta: initial, cfg: cfg, pub: pub}
}

func (f *raftMetadataFSM) Apply(l *raft.Log) interface{} {
	cmd, err := decodeCommand(l.Data)
	if err != nil {
		return err
	}

	f.mu.Lock()
	startVersion := f.meta.Version
	changed := false

	switch cmd.Type {
	case cmdAssignTopic:
		partitions := cmd.TopicConfig.Partitions
		if partitions <= 0 {
			partitions = 1
		}

		newParts := assignTopicPartitions(f.cfg, f.meta.Brokers, cmd.Topic, partitions, cmd.TopicConfig.ReplicationFactor, f.meta.Partitions)
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

	meta := f.meta
	changed = changed || meta.Version != startVersion
	f.mu.Unlock()

	if err == nil && changed && f.pub != nil {
		f.pub.publish(meta)
	}

	return err
}

func (f *raftMetadataFSM) applyReplicaProgressLocked(
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) error {
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
			if b.Host == info.Host &&
				b.Port == info.Port &&
				b.Rack == info.Rack &&
				b.HTTPAddr == info.HTTPAddr &&
				b.ControllerAddr == info.ControllerAddr {
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

	if f.pub != nil {
		f.pub.publish(meta)
	}

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
	cfg     api.BrokerConfig
	fsm     *raftMetadataFSM
	raft    *raft.Raft
	pub     *metadataPublisher
	closers []io.Closer
}

// NewRaftController bootstraps a Raft instance with given initial metadata.
func NewRaftController(cfg api.BrokerConfig, initialMeta api.ClusterMetadata, raftDir string) (*RaftController, error) {
	rCfg := raft.DefaultConfig()
	useInmem := cfg.RaftBindAddr == "" || len(cfg.RaftPeers) == 0

	var (
		localAddr raft.ServerAddress
		localID   raft.ServerID
	)

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

	logStore, stableStore, snapStore, closers, err := buildStores(raftDir)
	if err != nil {
		return nil, err
	}

	transport, resolvedAddr, err := newRaftTransport(cfg, rCfg, useInmem)
	if err != nil {
		return nil, err
	}

	initial, err := resolveInitialMetadata(stableStore, initialMeta)
	if err != nil {
		return nil, err
	}

	if err := bootstrapClusterIfNeeded(
		cfg, rCfg, logStore, stableStore, snapStore, transport, resolvedAddr, initial,
	); err != nil {
		return nil, err
	}

	pub := metadataPublisher{}
	fsm := newRaftMetadataFSM(cfg, initial, &pub)

	r, err := raft.NewRaft(rCfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, err
	}

	return &RaftController{cfg: cfg, fsm: fsm, raft: r, pub: &pub, closers: closers}, nil
}

func newRaftTransport(cfg api.BrokerConfig, rCfg *raft.Config, useInmem bool) (raft.Transport, raft.ServerAddress, error) {
	if useInmem {
		addr, inmem := raft.NewInmemTransport(raft.ServerAddress(rCfg.LocalID))
		return inmem, addr, nil
	}

	tcpTransport, err := raft.NewTCPTransport(cfg.RaftBindAddr, nil, 3, 2*time.Second, io.Discard)
	if err != nil {
		return nil, "", err
	}

	return tcpTransport, raft.ServerAddress(cfg.RaftBindAddr), nil
}

func resolveInitialMetadata(stableStore raft.StableStore, initialMeta api.ClusterMetadata) (api.ClusterMetadata, error) {
	stored, err := loadInitialMetadata(stableStore)
	if err != nil {
		return api.ClusterMetadata{}, err
	}

	initial := initialMeta

	if stored != nil {
		return *stored, nil
	}

	if err := persistInitialMetadata(stableStore, initial); err != nil {
		return api.ClusterMetadata{}, err
	}

	return initial, nil
}

func bootstrapClusterIfNeeded(
	cfg api.BrokerConfig,
	rCfg *raft.Config,
	logStore raft.LogStore,
	stableStore raft.StableStore,
	snapStore raft.SnapshotStore,
	transport raft.Transport,
	localAddr raft.ServerAddress,
	initial api.ClusterMetadata,
) error {
	hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		return err
	}

	if hasState {
		return nil
	}

	if len(cfg.RaftPeers) == 0 {
		cfg.RaftPeers = []string{string(localAddr)}
	}

	config := raft.Configuration{
		Servers: buildServers(cfg, rCfg.LocalID, localAddr),
	}

	if err := persistInitialMetadata(stableStore, initial); err != nil {
		return err
	}

	if err := raft.BootstrapCluster(rCfg, logStore, stableStore, snapStore, transport, config); err != nil && err != raft.ErrCantBootstrap {
		return err
	}

	return nil
}

func buildStores(raftDir string) (raft.LogStore, raft.StableStore, raft.SnapshotStore, []io.Closer, error) {
	if raftDir == "" || raceDetectorEnabled() {
		store := raft.NewInmemStore()
		return store, store, raft.NewInmemSnapshotStore(), nil, nil
	}

	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, nil, nil, nil, err
	}

	dbPath := filepath.Join(raftDir, "raft.bolt")

	logStore, err := raftboltdb.NewBoltStore(dbPath)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	snapStore, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
	if err != nil {
		return nil, nil, nil, []io.Closer{logStore}, err
	}

	return logStore, logStore, snapStore, []io.Closer{logStore}, nil
}

var initialMetadataKey = []byte("metadata/initial")

func persistInitialMetadata(store raft.StableStore, meta api.ClusterMetadata) error {
	if store == nil {
		return nil
	}

	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	return store.Set(initialMetadataKey, b)
}

func loadInitialMetadata(store raft.StableStore) (*api.ClusterMetadata, error) {
	if store == nil {
		return nil, nil
	}

	b, err := store.Get(initialMetadataKey)
	if err != nil {
		if errors.Is(err, raftboltdb.ErrKeyNotFound) || err.Error() == "not found" {
			return nil, nil
		}

		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var meta api.ClusterMetadata
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

func (c *RaftController) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	c.fsm.mu.Lock()
	defer c.fsm.mu.Unlock()

	return c.fsm.meta, nil
}

func (c *RaftController) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	meta, _ := c.GetClusterMetadata(ctx)
	ch := c.pub.watch(ctx, sinceVersion, meta)

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

func (c *RaftController) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition, brokerID int,
	lastOffset, leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
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
			return c.normalizeApplyError(err)
		}

		if applyErr, ok := future.Response().(error); ok && applyErr != nil {
			return c.normalizeApplyError(applyErr)
		}

		return nil
	}
}

func (c *RaftController) normalizeApplyError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, raft.ErrNotLeader) {
		return NotLeaderError{Leader: c.leaderHint()}
	}

	return err
}

func (c *RaftController) waitForLeader(ctx context.Context) error {
	if c.raft.State() == raft.Leader {
		return nil
	}

	if leader := c.leaderHint(); leader != "" {
		return NotLeaderError{Leader: leader}
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			if leader := c.leaderHint(); leader != "" {
				return NotLeaderError{Leader: leader}
			}

			return ErrLeaderNotElected
		case <-ticker.C:
			if c.raft.State() == raft.Leader {
				return nil
			}

			if leader := c.leaderHint(); leader != "" {
				return NotLeaderError{Leader: leader}
			}
		}
	}
}

func (c *RaftController) leaderHint() string {
	addr, id := c.raft.LeaderWithID()
	if id != "" {
		return string(id)
	}

	return string(addr)
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

// RaftLeader returns the current raft leader address as string.
func (c *RaftController) RaftLeader() string {
	return string(c.raft.Leader())
}

// RaftLeaderID returns the current raft leader ID.
func (c *RaftController) RaftLeaderID() string {
	_, id := c.raft.LeaderWithID()

	return string(id)
}

// RaftTerm returns the current term.
func (c *RaftController) RaftTerm() uint64 {
	stats := c.raft.Stats()
	if termStr, ok := stats["term"]; ok {
		term, err := strconv.ParseUint(termStr, 10, 64)
		if err == nil {
			return term
		}
	}

	return 0
}

// RaftPeers returns peer ids and addresses.
func (c *RaftController) RaftPeers() []PeerInfo {
	servers := c.raft.GetConfiguration().Configuration().Servers

	out := make([]PeerInfo, 0, len(servers))
	for _, s := range servers {
		out = append(out, PeerInfo{ID: string(s.ID), Address: string(s.Address)})
	}

	return out
}

// Close stops the underlying Raft instance.
func (c *RaftController) Close() error {
	f := c.raft.Shutdown()
	err := f.Error()

	for _, cl := range c.closers {
		_ = cl.Close()
	}

	return err
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
