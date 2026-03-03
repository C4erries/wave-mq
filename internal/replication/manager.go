package replication

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/c4erries/wave-mq/internal/controller"
	"github.com/c4erries/wave-mq/internal/storage"
	"github.com/c4erries/wave-mq/pkg/api"
)

type Manager struct {
	cfg   api.BrokerConfig
	store *storage.Manager
	ctrl  controller.MetadataStore
	repl  Replicator

	sinkFactory   SinkFactory
	workerFactory WorkerFactory

	mu      sync.Mutex
	running map[partitionKey]runningReplicator
}

type partitionKey struct {
	topic     string
	partition int
}

type runningReplicator struct {
	cancel     context.CancelFunc
	assignment api.PartitionAssignment
}

type ReplicationWorker interface {
	Run(ctx context.Context) error
}

type (
	SinkFactory   func(topic string, partition int) Sink
	WorkerFactory func(repl Replicator, leader api.BrokerInfo, topic string, partition int, sink Sink) ReplicationWorker
)

// NewManager prepares a replication manager.
func NewManager(cfg api.BrokerConfig, store *storage.Manager, ctrl controller.MetadataStore, repl Replicator) *Manager {
	return NewManagerWithFactories(cfg, store, ctrl, repl, nil, nil)
}

// NewManagerWithFactories prepares a replication manager with injectable worker/sink factories.
func NewManagerWithFactories(
	cfg api.BrokerConfig,
	store *storage.Manager,
	ctrl controller.MetadataStore,
	repl Replicator,
	sinkFactory SinkFactory,
	workerFactory WorkerFactory,
) *Manager {
	if sinkFactory == nil {
		sinkFactory = func(topic string, partition int) Sink {
			return NewWALSink(store, topic, partition)
		}
	}

	if workerFactory == nil {
		workerFactory = func(repl Replicator, leader api.BrokerInfo, topic string, partition int, sink Sink) ReplicationWorker {
			return NewPartitionReplicator(repl, leader, topic, partition, sink)
		}
	}

	return &Manager{
		cfg:           cfg,
		store:         store,
		ctrl:          ctrl,
		repl:          repl,
		sinkFactory:   sinkFactory,
		workerFactory: workerFactory,
		running:       make(map[partitionKey]runningReplicator),
	}
}

// Run watches cluster metadata and starts/stops partition replicators accordingly.
func (m *Manager) Run(ctx context.Context) error {
	if m.ctrl == nil || m.repl == nil || m.store == nil {
		return fmt.Errorf("replication manager missing dependencies")
	}

	updates, err := m.ctrl.WatchClusterMetadata(ctx, 0)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return ctx.Err()
		case meta, ok := <-updates:
			if !ok {
				m.stopAll()
				return nil
			}

			m.applyMetadata(ctx, meta)
		}
	}
}

func (m *Manager) applyMetadata(ctx context.Context, meta api.ClusterMetadata) {
	desired := make(map[partitionKey]api.PartitionAssignment)

	for _, p := range meta.Partitions {
		if containsInt(p.Replicas, m.cfg.BrokerID) && p.Leader != m.cfg.BrokerID {
			key := partitionKey{topic: p.Topic, partition: p.Partition}
			desired[key] = p
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop replicators no longer needed or whose assignments changed.
	for key, running := range m.running {
		desiredAssign, ok := desired[key]
		if !ok || !sameAssignment(running.assignment, desiredAssign) {
			running.cancel()
			delete(m.running, key)
		}
	}
	// Start new replicators.
	for key, p := range desired {
		if _, ok := m.running[key]; ok {
			continue
		}

		m.startReplicator(ctx, meta.Brokers, key, p)
	}
}

func (m *Manager) startReplicator(
	ctx context.Context,
	brokers []api.BrokerInfo,
	key partitionKey,
	assign api.PartitionAssignment,
) {
	leaderInfo := findBroker(brokers, assign.Leader)
	if leaderInfo == nil {
		return
	}

	ctxRep, cancel := context.WithCancel(ctx)
	sink := m.sinkFactory(assign.Topic, assign.Partition)
	sink = NewReportingSink(sink, m.ctrl, assign.Topic, assign.Partition, m.cfg.BrokerID)

	worker := m.workerFactory(m.repl, *leaderInfo, assign.Topic, assign.Partition, sink)
	go func(repKey partitionKey, assignment api.PartitionAssignment) {
		_ = worker.Run(ctxRep)
		// Once the replicator exits, clean up the reference if still present.
		m.mu.Lock()
		if current, ok := m.running[repKey]; ok && sameAssignment(current.assignment, assignment) {
			delete(m.running, repKey)
		}
		m.mu.Unlock()
	}(key, assign)

	m.running[key] = runningReplicator{cancel: cancel, assignment: assign}
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, running := range m.running {
		running.cancel()
		delete(m.running, key)
	}
}

func sameAssignment(a, b api.PartitionAssignment) bool {
	if a.Topic != b.Topic || a.Partition != b.Partition || a.Leader != b.Leader {
		return false
	}

	return slices.Equal(a.Replicas, b.Replicas)
}

func findBroker(list []api.BrokerInfo, id int) *api.BrokerInfo {
	for _, b := range list {
		if b.BrokerID == id {
			cp := b
			return &cp
		}
	}

	return nil
}

func containsInt(list []int, id int) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}

	return false
}
