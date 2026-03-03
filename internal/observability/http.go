package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartHTTPServer launches an HTTP server exposing metrics, healthz, pprof and optional extra handlers.
// readyFunc indicates readiness; if nil, readiness is always true.
// onStarted is called after listener bind succeeds (can be nil).
func StartHTTPServer(ctx context.Context, addr string, readyFunc func() bool, extra func(mux *http.ServeMux), onStarted func()) error {
	if addr == "" {
		return fmt.Errorf("http addr is required")
	}

	if readyFunc == nil {
		readyFunc = func() bool { return true }
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if readyFunc() {
			if err := writeHealthResponse(w, http.StatusOK, "ok"); err != nil {
				RequestErrors.WithLabelValues("observability", "healthz_write").Inc()
				slog.Error("observability health response write failed", "status", http.StatusOK, "err", err)
			}

			return
		}

		if err := writeHealthResponse(w, http.StatusServiceUnavailable, "not ready"); err != nil {
			RequestErrors.WithLabelValues("observability", "healthz_write").Inc()
			slog.Error("observability health response write failed", "status", http.StatusServiceUnavailable, "err", err)
		}
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	if extra != nil {
		extra(mux)
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)

	if onStarted != nil {
		onStarted()
	}

	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
			RequestErrors.WithLabelValues("observability", "shutdown").Inc()
			return shutdownErr
		}

		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}

		return err
	}
}

func writeHealthResponse(w http.ResponseWriter, status int, body string) error {
	w.WriteHeader(status)

	_, err := w.Write([]byte(body))

	return err
}
