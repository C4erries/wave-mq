package observability

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartHTTPServer launches an HTTP server exposing metrics, healthz, pprof and optional extra handlers.
// readyFunc indicates readiness; if nil, readiness is always true.
// onStarted is called after ListenAndServe begins successfully (can be nil).
func StartHTTPServer(ctx context.Context, addr string, readyFunc func() bool, extra func(mux *http.ServeMux), onStarted func()) error {
	if addr == "" {
		return fmt.Errorf("http addr is required")
	}

	if readyFunc == nil {
		readyFunc = func() bool { return true }
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if readyFunc() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))

			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
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

	errCh := make(chan error, 1)

	go func() {
		if onStarted != nil {
			onStarted()
		}

		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdownCtx)

		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}

		return err
	}
}
