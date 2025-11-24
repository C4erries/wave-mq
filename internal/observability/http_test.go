package observability

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestHTTPServerHealthAndMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := "127.0.0.1:18080"
	ready := func() bool { return true }
	errCh := make(chan error, 1)
	go func() {
		errCh <- StartHTTPServer(ctx, addr, ready)
	}()

	// Give server a moment to start
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("healthz request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("metrics request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	if len(body) == 0 {
		t.Fatalf("metrics body empty")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func TestHealthzNotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := "127.0.0.1:18081"
	ready := func() bool { return false }
	errCh := make(chan error, 1)
	go func() {
		errCh <- StartHTTPServer(ctx, addr, ready)
	}()
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("healthz request: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}
