package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/c4erries/wave-mq/pkg/api"
)

type commandType uint8

const (
	cmdAssignTopic commandType = iota + 1
	cmdReportReplicaProgress
)

type raftCommand struct {
	Type commandType

	Topic string
	Part  int

	Broker int

	TopicConfig api.TopicConfig

	LastOffset          api.Offset
	LeaderHighWatermark api.Offset
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

// RaftController replicates cluster metadata through a Raft log (single-node in this step).
type RaftController struct {
	cfg  api.BrokerConfig
	fsm  *raftMetadataFSM
	raft *raft.Raft
}

// NewRaftController bootstraps a single-node Raft instance with given initial metadata.
func NewRaftController(cfg api.BrokerConfig, initialMeta api.ClusterMetadata, raftDir string) (*RaftController, error) {
	rCfg := raft.DefaultConfig()
	rCfg.LocalID = raft.ServerID(fmt.Sprintf("broker-%d", cfg.BrokerID))
	rCfg.SnapshotInterval = 2 * time.Second
	rCfg.SnapshotThreshold = 64
	rCfg.HeartbeatTimeout = 50 * time.Millisecond
	rCfg.ElectionTimeout = 50 * time.Millisecond
	rCfg.LeaderLeaseTimeout = 50 * time.Millisecond
	rCfg.CommitTimeout = 10 * time.Millisecond

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewInmemSnapshotStore()
	transportAddr, transport := raft.NewInmemTransport(raft.ServerAddress(rCfg.LocalID))
	config := raft.Configuration{
		Servers: []raft.Server{
			{Suffrage: raft.Voter, ID: rCfg.LocalID, Address: transportAddr},
		},
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
	_ = ctx
	_ = info
	// TODO: replicate broker registration via Raft when multi-node is needed.
	return nil
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
	timeout := time.After(2 * time.Second)
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
