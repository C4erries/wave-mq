package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/storage"
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
	if err := postRegisterBrokerToLeader(context.Background(), logger, nil, leaderAddr, info); err != nil {
		t.Fatalf("postRegisterBrokerToLeader: %v", err)
	}

	if got := payload["httpAddr"]; got != info.HTTPAddr {
		t.Fatalf("payload httpAddr = %v, want %q", got, info.HTTPAddr)
	}

	if got := payload["controllerAddr"]; got != info.ControllerAddr {
		t.Fatalf("payload controllerAddr = %v, want %q", got, info.ControllerAddr)
	}
}

type fakeCtrl struct {
	meta api.ClusterMetadata
}

func (f *fakeCtrl) GetClusterMetadata(ctx context.Context) (api.ClusterMetadata, error) {
	_ = ctx

	return f.meta, nil
}

func (f *fakeCtrl) WatchClusterMetadata(ctx context.Context, sinceVersion int64) (<-chan api.ClusterMetadata, error) {
	_ = ctx
	_ = sinceVersion

	ch := make(chan api.ClusterMetadata)
	close(ch)

	return ch, nil
}

func (f *fakeCtrl) RegisterBroker(ctx context.Context, info api.BrokerInfo) error {
	_ = ctx
	_ = info

	return nil
}

func (f *fakeCtrl) AssignTopic(ctx context.Context, name string, cfg api.TopicConfig) (api.ClusterMetadata, error) {
	_ = ctx
	_ = name
	_ = cfg

	return f.meta, nil
}

func (f *fakeCtrl) ReportReplicaProgress(
	ctx context.Context,
	topic string,
	partition int,
	brokerID int,
	lastOffset api.Offset,
	leaderHighWatermark api.Offset,
) (api.ClusterMetadata, error) {
	_ = ctx
	_ = topic
	_ = partition
	_ = brokerID
	_ = lastOffset
	_ = leaderHighWatermark

	return f.meta, nil
}

func TestLeaderRegisterBrokerURLUsesLeaderHTTPAddrFromMetadata(t *testing.T) {
	leaderAddr := "leader:9001"

	ctrl := &fakeCtrl{
		meta: api.ClusterMetadata{
			Brokers: []api.BrokerInfo{
				{BrokerID: 1, HTTPAddr: "leader:18090", ControllerAddr: leaderAddr},
			},
		},
	}

	url, err := leaderRegisterBrokerURL(context.Background(), ctrl, leaderAddr, api.BrokerInfo{HTTPAddr: "self:8090"})
	if err != nil {
		t.Fatalf("leaderRegisterBrokerURL: %v", err)
	}

	if want := "http://leader:18090/api/controller/brokers"; url != want {
		t.Fatalf("url=%q want=%q", url, want)
	}
}

func TestParseStartupOptions(t *testing.T) {
	opts, err := parseStartupOptions([]string{
		"-broker-id", "5",
		"-retention-hours", "3",
		"-raft-peer", "b1:9001,b2:9001",
		"-sync-on-append=false",
	})
	if err != nil {
		t.Fatalf("parseStartupOptions: %v", err)
	}

	if opts.cfg.BrokerID != 5 {
		t.Fatalf("broker id = %d, want 5", opts.cfg.BrokerID)
	}

	if opts.cfg.RetentionTime != 3*time.Hour {
		t.Fatalf("retention time = %s, want 3h", opts.cfg.RetentionTime)
	}

	if opts.syncOnAppend {
		t.Fatal("syncOnAppend should be false")
	}

	if len(opts.cfg.RaftPeers) != 2 {
		t.Fatalf("raft peers len=%d, want 2", len(opts.cfg.RaftPeers))
	}
}

func TestValidateBrokerConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name    string
		cfg     api.BrokerConfig
		wantErr string
	}{
		{
			name: "single mode",
			cfg:  api.BrokerConfig{ControllerMode: "single"},
		},
		{
			name:    "raft missing bind",
			cfg:     api.BrokerConfig{ControllerMode: "raft", RaftPeers: []string{"n1:9001"}},
			wantErr: "-raft-bind",
		},
		{
			name:    "raft missing peers",
			cfg:     api.BrokerConfig{ControllerMode: "raft", RaftBindAddr: "n1:9001"},
			wantErr: "-raft-peer",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBrokerConfig(logger, tc.cfg)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v, want contains %q", err, tc.wantErr)
				}
			}
		})
	}
}

func TestRunReturnsStorageInitError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	want := errors.New("boom")

	factory := defaultAppFactory()
	factory.newStorageManager = func(cfg storage.Config) (*storage.Manager, error) {
		_ = cfg
		return nil, want
	}
	factory.waitForSignal = func() {}

	err := run(context.Background(), logger, startupOptions{
		cfg: api.BrokerConfig{
			DataDir:         t.TempDir(),
			MaxSegmentBytes: 1024,
		},
		syncOnAppend: true,
	}, factory)
	if !errors.Is(err, want) {
		t.Fatalf("run err = %v, want %v", err, want)
	}
}
