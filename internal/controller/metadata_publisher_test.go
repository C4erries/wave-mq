package controller

import (
	"context"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestMetadataPublisherPublishDoesNotBlockOnSlowWatcher(t *testing.T) {
	t.Parallel()

	var pub metadataPublisher
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := pub.watch(ctx, 0, api.ClusterMetadata{Version: 1})

	done := make(chan struct{})
	go func() {
		pub.publish(api.ClusterMetadata{Version: 2})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("publish blocked on slow watcher")
	}

	select {
	case meta := <-ch:
		if meta.Version != 2 {
			t.Fatalf("expected latest version 2, got %d", meta.Version)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for published metadata")
	}
}

func TestMetadataPublisherClosesWatcherOnCancel(t *testing.T) {
	t.Parallel()

	var pub metadataPublisher
	ctx, cancel := context.WithCancel(context.Background())

	ch := pub.watch(ctx, 0, api.ClusterMetadata{Version: 1})

	// Drain initial snapshot to avoid ambiguity when asserting closure.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for initial metadata")
	}

	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed watcher channel after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher channel did not close after cancel")
	}
}
