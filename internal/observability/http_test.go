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
		errCh <- StartHTTPServer(ctx, addr, ready, nil, nil)
	}()

	resp := waitHTTPStatus(t, "http://"+addr+"/healthz", http.StatusOK, 2*time.Second)
	defer resp.Body.Close()

	resp.Body.Close()

	resp = waitHTTPStatus(t, "http://"+addr+"/metrics", http.StatusOK, 2*time.Second)
	defer resp.Body.Close()

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
		errCh <- StartHTTPServer(ctx, addr, ready, nil, nil)
	}()

	resp := waitHTTPStatus(t, "http://"+addr+"/healthz", http.StatusServiceUnavailable, 2*time.Second)
	defer resp.Body.Close()

	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func waitHTTPStatus(t *testing.T, url string, want int, timeout time.Duration) *http.Response {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		resp, err := http.Get(url)
		if err == nil {
			if resp.StatusCode == want {
				return resp
			}

			_ = resp.Body.Close()
		}

		select {
		case <-timer.C:
			t.Fatalf("timeout waiting for %s status %d", url, want)
		case <-ticker.C:
		}
	}
}
