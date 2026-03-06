package main

import "testing"

func TestValidateBenchConfig(t *testing.T) {
	t.Parallel()

	valid := benchConfig{
		brokerAddr:  "127.0.0.1:7912",
		topic:       "bench",
		partition:   0,
		messages:    10,
		valueSize:   16,
		concurrency: 2,
	}

	testCases := []struct {
		name    string
		cfg     benchConfig
		wantErr bool
	}{
		{name: "valid", cfg: valid},
		{name: "missing broker", cfg: benchConfig{topic: "bench", partition: 0, messages: 1, valueSize: 1, concurrency: 1}, wantErr: true},
		{name: "missing topic", cfg: benchConfig{brokerAddr: "127.0.0.1:7912", partition: 0, messages: 1, valueSize: 1, concurrency: 1}, wantErr: true},
		{name: "negative partition", cfg: benchConfig{brokerAddr: "127.0.0.1:7912", topic: "bench", partition: -1, messages: 1, valueSize: 1, concurrency: 1}, wantErr: true},
		{name: "non-positive messages", cfg: benchConfig{brokerAddr: "127.0.0.1:7912", topic: "bench", partition: 0, messages: 0, valueSize: 1, concurrency: 1}, wantErr: true},
		{name: "non-positive value-size", cfg: benchConfig{brokerAddr: "127.0.0.1:7912", topic: "bench", partition: 0, messages: 1, valueSize: 0, concurrency: 1}, wantErr: true},
		{name: "non-positive concurrency", cfg: benchConfig{brokerAddr: "127.0.0.1:7912", topic: "bench", partition: 0, messages: 1, valueSize: 1, concurrency: 0}, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateBenchConfig(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("validateBenchConfig(%+v) expected error", tc.cfg)
				}

				return
			}

			if err != nil {
				t.Fatalf("validateBenchConfig(%+v) err = %v", tc.cfg, err)
			}
		})
	}
}

func TestParseFlags(t *testing.T) {
	t.Parallel()

	cfg, err := parseFlags([]string{
		"-broker", "10.0.0.1:7912",
		"-topic", "topic-a",
		"-partition", "3",
		"-messages", "50",
		"-value-size", "256",
		"-concurrency", "8",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}

	if cfg.brokerAddr != "10.0.0.1:7912" {
		t.Fatalf("brokerAddr = %q", cfg.brokerAddr)
	}

	if cfg.topic != "topic-a" {
		t.Fatalf("topic = %q", cfg.topic)
	}

	if cfg.partition != 3 {
		t.Fatalf("partition = %d", cfg.partition)
	}

	if cfg.messages != 50 {
		t.Fatalf("messages = %d", cfg.messages)
	}

	if cfg.valueSize != 256 {
		t.Fatalf("valueSize = %d", cfg.valueSize)
	}

	if cfg.concurrency != 8 {
		t.Fatalf("concurrency = %d", cfg.concurrency)
	}
}

func TestBuildWorkQueueCount(t *testing.T) {
	t.Parallel()

	const want = 7

	queue := buildWorkQueue(want)

	got := 0
	for range queue {
		got++
	}

	if got != want {
		t.Fatalf("buildWorkQueue count = %d, want %d", got, want)
	}
}
