package api

import (
	"strings"
	"testing"
	"time"
)

func TestBrokerConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     BrokerConfig
		wantErr string
	}{
		{
			name: "valid single mode",
			cfg: BrokerConfig{
				BrokerID:          1,
				ReplicationFactor: 1,
				ControllerMode:    ControllerModeSingle,
				RetentionBytes:    -1,
			},
		},
		{
			name: "invalid broker id",
			cfg: BrokerConfig{
				BrokerID:          0,
				ReplicationFactor: 1,
			},
			wantErr: "broker id",
		},
		{
			name: "invalid replication factor",
			cfg: BrokerConfig{
				BrokerID:          1,
				ReplicationFactor: 0,
			},
			wantErr: "replication factor",
		},
		{
			name: "invalid controller mode",
			cfg: BrokerConfig{
				BrokerID:          1,
				ReplicationFactor: 1,
				ControllerMode:    "weird",
			},
			wantErr: "controller mode",
		},
		{
			name: "negative retention time",
			cfg: BrokerConfig{
				BrokerID:          1,
				ReplicationFactor: 1,
				RetentionTime:     -time.Second,
			},
			wantErr: "retention time",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() unexpected error: %v", err)
				}

				return
			}

			if err == nil {
				t.Fatalf("Validate() expected error containing %q", tc.wantErr)
			}

			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() err=%q, want contains %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestTopicConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     TopicConfig
		wantErr string
	}{
		{
			name: "valid",
			cfg: TopicConfig{
				Partitions:        1,
				ReplicationFactor: 1,
				RetentionBytes:    -1,
			},
		},
		{
			name: "invalid partitions",
			cfg: TopicConfig{
				Partitions:        0,
				ReplicationFactor: 1,
			},
			wantErr: "partitions",
		},
		{
			name: "invalid replication factor",
			cfg: TopicConfig{
				Partitions:        1,
				ReplicationFactor: 0,
			},
			wantErr: "replication factor",
		},
		{
			name: "invalid retention bytes",
			cfg: TopicConfig{
				Partitions:        1,
				ReplicationFactor: 1,
				RetentionBytes:    -2,
			},
			wantErr: "retention bytes",
		},
		{
			name: "invalid retention time",
			cfg: TopicConfig{
				Partitions:        1,
				ReplicationFactor: 1,
				RetentionTime:     -time.Millisecond,
			},
			wantErr: "retention time",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() unexpected error: %v", err)
				}

				return
			}

			if err == nil {
				t.Fatalf("Validate() expected error containing %q", tc.wantErr)
			}

			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() err=%q, want contains %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestIsValidControllerMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mode  string
		valid bool
	}{
		{mode: "", valid: true},
		{mode: ControllerModeSingle, valid: true},
		{mode: ControllerModeRaft, valid: true},
		{mode: "invalid", valid: false},
	}

	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()

			if got := IsValidControllerMode(tc.mode); got != tc.valid {
				t.Fatalf("IsValidControllerMode(%q)=%v want=%v", tc.mode, got, tc.valid)
			}
		})
	}
}
