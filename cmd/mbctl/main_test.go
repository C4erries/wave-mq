package main

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestRunCLIShowsUsageWithoutArgs(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	err := runCLI(nil, &out, &errOut)
	if err != nil {
		t.Fatalf("runCLI() err = %v", err)
	}

	if got := errOut.String(); !strings.Contains(got, "Usage: mbctl") {
		t.Fatalf("usage not printed, got %q", got)
	}
}

func TestRunCLIUnknownCommand(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	err := runCLI([]string{"unknown"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected error for unknown command")
	}

	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := errOut.String(); !strings.Contains(got, "Usage: mbctl") {
		t.Fatalf("usage not printed, got %q", got)
	}
}

func TestRunCLIProduceRequiresTopic(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	err := runCLI([]string{"produce", "-value", "v1"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected validation error")
	}

	if !strings.Contains(err.Error(), "topic is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunCLIFetchRejectsTooLargeMaxBytes(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	err := runCLI([]string{"fetch", "-topic", "t1", "-max-bytes", "2147483648"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected max-bytes validation error")
	}

	if !strings.Contains(err.Error(), "max-bytes out of int32 range") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestToInt32(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		value   int
		want    int32
		wantErr bool
	}{
		{name: "zero", value: 0, want: 0},
		{name: "max int32", value: math.MaxInt32, want: math.MaxInt32},
		{name: "negative", value: -1, wantErr: true},
		{name: "overflow", value: math.MaxInt32 + 1, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := toInt32(tc.value, "field")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("toInt32(%d) expected error", tc.value)
				}

				return
			}

			if err != nil {
				t.Fatalf("toInt32(%d) err = %v", tc.value, err)
			}

			if got != tc.want {
				t.Fatalf("toInt32(%d) = %d, want %d", tc.value, got, tc.want)
			}
		})
	}
}
