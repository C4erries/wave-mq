package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunCLIShowsUsageWithoutArgs(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	err := runCLI(nil, &out, &errOut)
	if err != nil {
		t.Fatalf("runCLI() err = %v", err)
	}

	if got := errOut.String(); !strings.Contains(got, "Usage: mbctl") {
		t.Fatalf("usage not printed, got %q", got)
	}
}

func TestRunCLIUnknownCommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

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
	var out bytes.Buffer
	var errOut bytes.Buffer

	err := runCLI([]string{"produce", "-value", "v1"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected validation error")
	}

	if !strings.Contains(err.Error(), "topic is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunCLIFetchRejectsTooLargeMaxBytes(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	err := runCLI([]string{"fetch", "-topic", "t1", "-max-bytes", "2147483648"}, &out, &errOut)
	if err == nil {
		t.Fatal("expected max-bytes validation error")
	}

	if !strings.Contains(err.Error(), "max-bytes out of int32 range") {
		t.Fatalf("unexpected error: %v", err)
	}
}
