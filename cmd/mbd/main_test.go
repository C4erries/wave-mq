package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/c4erries/wave-mq/pkg/api"
)

func TestAdvertisedHTTPAddrUsesAdvertisedHost(t *testing.T) {
	cfg := api.BrokerConfig{
		AdvertisedAddr: "broker1:7912",
		RaftBindAddr:   "broker1:9001",
		BinaryAddr:     ":7912",
		HTTPAddr:       ":8090",
	}

	got := advertisedHTTPAddr(cfg)

	want := "broker1:8090"
	if got != want {
		t.Fatalf("advertisedHTTPAddr() = %q, want %q", got, want)
	}
}

func TestAdvertisedHTTPAddrFallsBackToRaftBindHost(t *testing.T) {
	cfg := api.BrokerConfig{
		RaftBindAddr: "broker2:9001",
		BinaryAddr:   ":7912",
		HTTPAddr:     ":8090",
	}

	got := advertisedHTTPAddr(cfg)

	want := "broker2:8090"
	if got != want {
		t.Fatalf("advertisedHTTPAddr() = %q, want %q", got, want)
	}
}

func TestPostRegisterBrokerToLeaderSendsExtendedPayload(t *testing.T) {
	var payload map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}

		if r.URL.Path != "/api/controller/brokers" {
			t.Fatalf("path = %s, want /api/controller/brokers", r.URL.Path)
		}

		defer r.Body.Close()

		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	serverHost := strings.TrimPrefix(server.URL, "http://")
	info := api.BrokerInfo{
		BrokerID:       2,
		Host:           "broker2:7912",
		HTTPAddr:       serverHost,
		ControllerAddr: "broker2:9001",
	}

	leaderAddr := serverHost
	if err := postRegisterBrokerToLeader(context.Background(), logger, leaderAddr, info); err != nil {
		t.Fatalf("postRegisterBrokerToLeader: %v", err)
	}

	if got := payload["httpAddr"]; got != info.HTTPAddr {
		t.Fatalf("payload httpAddr = %v, want %q", got, info.HTTPAddr)
	}

	if got := payload["controllerAddr"]; got != info.ControllerAddr {
		t.Fatalf("payload controllerAddr = %v, want %q", got, info.ControllerAddr)
	}
}
