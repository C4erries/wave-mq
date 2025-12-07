package replication

import (
	"context"
	"fmt"
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

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

// NewManager prepares a replication manager.
func NewManager(cfg api.BrokerConfig, store *storage.Manager, ctrl controller.MetadataStore, repl Replicator) *Manager {
	return &Manager{
		cfg:     cfg,
		store:   store,
		ctrl:    ctrl,
		repl:    repl,
		running: make(map[string]context.CancelFunc),
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
	desired := make(map[string]api.PartitionAssignment)
	for _, p := range meta.Partitions {
		if containsInt(p.Replicas, m.cfg.BrokerID) && p.Leader != m.cfg.BrokerID {
			key := fmt.Sprintf("%s:%d", p.Topic, p.Partition)
			desired[key] = p
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop replicators no longer needed.
	for key, cancel := range m.running {
		if _, ok := desired[key]; !ok {
			cancel()
			delete(m.running, key)
		}
	}
	// Start new replicators.
	for key, p := range desired {
		if _, ok := m.running[key]; ok {
			continue
		}
		leaderInfo := findBroker(meta.Brokers, p.Leader)
		if leaderInfo == nil {
			continue
		}
		ctxRep, cancel := context.WithCancel(ctx)
		sink := NewWALSink(m.store, p.Topic, p.Partition)
		sink = NewReportingSink(sink, m.ctrl, p.Topic, p.Partition, m.cfg.BrokerID)
		pr := NewPartitionReplicator(m.repl, *leaderInfo, p.Topic, p.Partition, sink)
		go func() {
			_ = pr.Run(ctxRep)
		}()
		m.running[key] = cancel
	}
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, cancel := range m.running {
		cancel()
		delete(m.running, key)
	}
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
