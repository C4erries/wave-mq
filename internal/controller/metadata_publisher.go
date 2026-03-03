package controller

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/c4erries/wave-mq/pkg/api"
)

type metadataWatcher struct {
	ctx       context.Context
	ch        chan api.ClusterMetadata
	lastSent  atomic.Int64
	closeOnce sync.Once
}

type metadataPublisher struct {
	mu       sync.Mutex
	watchers []*metadataWatcher
}

func (p *metadataPublisher) watch(ctx context.Context, sinceVersion int64, initial api.ClusterMetadata) <-chan api.ClusterMetadata {
	w := &metadataWatcher{ctx: ctx, ch: make(chan api.ClusterMetadata, 1)}
	w.lastSent.Store(sinceVersion)
	p.mu.Lock()
	p.watchers = append(p.watchers, w)
	p.mu.Unlock()

	if initial.Version > sinceVersion {
		w.send(initial)
	}

	go func() {
		<-ctx.Done()
		p.removeWatcher(w)
	}()

	return w.ch
}

func (p *metadataPublisher) publish(meta api.ClusterMetadata) {
	p.mu.Lock()
	watchers := append([]*metadataWatcher(nil), p.watchers...)
	p.mu.Unlock()

	for _, w := range watchers {
		if meta.Version <= w.lastSent.Load() {
			continue
		}

		if !w.send(meta) {
			p.removeWatcher(w)
		}
	}
}

func (p *metadataPublisher) removeWatcher(target *metadataWatcher) {
	p.mu.Lock()
	for i, w := range p.watchers {
		if w == target {
			p.watchers = append(p.watchers[:i], p.watchers[i+1:]...)
			break
		}
	}
	p.mu.Unlock()
	target.close()
}

func (w *metadataWatcher) send(meta api.ClusterMetadata) bool {
	for {
		select {
		case <-w.ctx.Done():
			return false
		case w.ch <- meta:
			w.lastSent.Store(meta.Version)
			return true
		default:
		}

		// The channel buffer is full; drop the stale snapshot and retry so
		// publisher never blocks behind slow consumers.
		select {
		case <-w.ctx.Done():
			return false
		case <-w.ch:
		default:
		}
	}
}

func (w *metadataWatcher) close() {
	w.closeOnce.Do(func() { close(w.ch) })
}
