package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/pkg/api"
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

func TestRunCLIPingTextOutputUnchanged(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.now = stubNow(time.Unix(10, 0), time.Unix(10, 0))
	ctx.ping = func(addr string) (*netproto.PingResponse, error) {
		if addr != "127.0.0.1:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		return &netproto.PingResponse{Error: api.ErrNone}, nil
	}

	if err := runCLIWithContext([]string{"ping"}, ctx); err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	if got := out.String(); got != "pong rtt=0s\n" {
		t.Fatalf("unexpected text output: %q", got)
	}
}

func TestRunCLIPingJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.now = stubNow(time.Unix(10, 0), time.Unix(10, 0))
	ctx.ping = func(string) (*netproto.PingResponse, error) {
		return &netproto.PingResponse{Error: api.ErrNone}, nil
	}

	if err := runCLIWithContext([]string{"ping", "-json"}, ctx); err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got pingJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK || got.RTTMs != 0 {
		t.Fatalf("unexpected ping json: %+v", got)
	}
}

func TestRunCLICreateTopicJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.createTopic = func(addr string, req *netproto.CreateTopicRequest) (*netproto.CreateTopicResponse, error) {
		if addr != "broker:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		if req.Topic != "demo" || req.Partitions != 2 || req.ReplicationFactor != 1 {
			t.Fatalf("unexpected request: %+v", req)
		}

		return &netproto.CreateTopicResponse{Error: api.ErrNone}, nil
	}

	err := runCLIWithContext([]string{
		"create-topic", "-json", "-broker", "broker:7912", "-topic", "demo", "-partitions", "2", "-replication-factor", "1",
	}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got createTopicJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK || got.Topic != "demo" || got.Partitions != 2 || got.ReplicationFactor != 1 {
		t.Fatalf("unexpected create-topic json: %+v", got)
	}
}

func TestRunCLIProduceJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	now := time.Unix(20, 0)
	ctx.now = stubNow(now)
	ctx.produce = func(addr string, req *netproto.ProduceRequest) (*netproto.ProduceResponse, error) {
		if addr != "broker:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		if req.Topic != "demo" || req.Partition != 1 || len(req.Records) != 1 {
			t.Fatalf("unexpected request: %+v", req)
		}

		if string(req.Records[0].Key) != "k" || string(req.Records[0].Value) != "v" {
			t.Fatalf("unexpected record payload: %+v", req.Records[0])
		}

		if !req.Records[0].Timestamp.Equal(now) {
			t.Fatalf("unexpected record timestamp: %v", req.Records[0].Timestamp)
		}

		return &netproto.ProduceResponse{BaseOffset: 7, Error: api.ErrNone}, nil
	}

	err := runCLIWithContext([]string{
		"produce", "-json", "-broker", "broker:7912", "-topic", "demo", "-partition", "1", "-key", "k", "-value", "v",
	}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got produceJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK || got.BaseOffset != 7 {
		t.Fatalf("unexpected produce json: %+v", got)
	}
}

func TestRunCLIFetchTextOutputUnchanged(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.fetch = func(string, *netproto.FetchRequest) (*netproto.FetchResponse, error) {
		return &netproto.FetchResponse{
			Error: api.ErrNone,
			Records: []api.Record{
				{Offset: 1, Key: []byte("k"), Value: []byte("v")},
			},
		}, nil
	}

	err := runCLIWithContext([]string{"fetch", "-topic", "demo"}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	if got := out.String(); got != "offset=1 key=k value=v\n" {
		t.Fatalf("unexpected text output: %q", got)
	}
}

func TestRunCLIFetchJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.fetch = func(addr string, req *netproto.FetchRequest) (*netproto.FetchResponse, error) {
		if addr != "broker:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		if req.Topic != "demo" || req.Partition != 0 || req.Offset != 3 || req.MaxBytes != 1024 {
			t.Fatalf("unexpected request: %+v", req)
		}

		return &netproto.FetchResponse{
			Error:         api.ErrNone,
			HighWatermark: 9,
			Records: []api.Record{
				{Offset: 3, Key: []byte("k1"), Value: []byte("v1")},
				{Offset: 4, Key: []byte("k2"), Value: []byte("v2")},
			},
		}, nil
	}

	err := runCLIWithContext([]string{
		"fetch", "-json", "-broker", "broker:7912", "-topic", "demo", "-offset", "3", "-max-bytes", "1024",
	}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got fetchJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK || got.HighWatermark != 9 || len(got.Records) != 2 {
		t.Fatalf("unexpected fetch json: %+v", got)
	}

	if got.Records[0].Offset != 3 || got.Records[0].Key != "k1" || got.Records[0].Value != "v1" {
		t.Fatalf("unexpected first record: %+v", got.Records[0])
	}
}

func TestRunCLIMetadataJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.metadata = func(addr string, req *netproto.MetadataRequest) (*netproto.MetadataResponse, error) {
		if addr != "broker:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		if len(req.Topics) != 1 || req.Topics[0] != "demo" {
			t.Fatalf("unexpected request topics: %+v", req.Topics)
		}

		return &netproto.MetadataResponse{
			Error: api.ErrNone,
			Partitions: []api.PartitionMetadata{{
				Replica: api.PartitionReplica{
					Topic:       "demo",
					Partition:   0,
					BrokerID:    1,
					Role:        api.RoleLeader,
					LeaderEpoch: 4,
				},
				StartOffset:   2,
				HighWatermark: 8,
				Leader:        1,
				Replicas:      []int{1, 2},
				ISR:           []int{1},
			}},
		}, nil
	}

	err := runCLIWithContext([]string{"metadata", "-json", "-broker", "broker:7912", "-topic", "demo"}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got metadataJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK || len(got.Partitions) != 1 {
		t.Fatalf("unexpected metadata json: %+v", got)
	}

	part := got.Partitions[0]
	if part.Topic != "demo" || part.Role != "leader" || part.LeaderEpoch != 4 || part.HighWatermark != 8 {
		t.Fatalf("unexpected partition json: %+v", part)
	}

	if len(part.Replicas) != 2 || part.Replicas[1] != 2 || len(part.ISR) != 1 || part.ISR[0] != 1 {
		t.Fatalf("unexpected replica/isr json: %+v", part)
	}
}

func TestRunCLIListOffsetsJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.listOffsets = func(addr string, req *netproto.ListOffsetsRequest) (*netproto.ListOffsetsResponse, error) {
		if addr != "broker:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		if req.Topic != "demo" || req.Partition != 1 {
			t.Fatalf("unexpected request: %+v", req)
		}

		return &netproto.ListOffsetsResponse{Error: api.ErrNone, Earliest: 3, Latest: 11}, nil
	}

	err := runCLIWithContext([]string{"list-offsets", "-json", "-broker", "broker:7912", "-topic", "demo", "-partition", "1"}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got listOffsetsJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK || got.Earliest != 3 || got.Latest != 11 {
		t.Fatalf("unexpected list-offsets json: %+v", got)
	}
}

func TestRunCLICommitOffsetJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.commitOffset = func(addr string, req *netproto.CommitOffsetRequest) (*netproto.CommitOffsetResponse, error) {
		if addr != "broker:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		if req.Group != "g" || req.Topic != "demo" || req.Partition != 1 || req.Offset != 15 {
			t.Fatalf("unexpected request: %+v", req)
		}

		return &netproto.CommitOffsetResponse{Error: api.ErrNone}, nil
	}

	err := runCLIWithContext([]string{
		"commit-offset", "-json", "-broker", "broker:7912", "-group", "g", "-topic", "demo", "-partition", "1", "-offset", "15",
	}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got okJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK {
		t.Fatalf("unexpected commit-offset json: %+v", got)
	}
}

func TestRunCLIFetchCommittedJSON(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.fetchCommitted = func(addr string, req *netproto.FetchCommittedRequest) (*netproto.FetchCommittedResponse, error) {
		if addr != "broker:7912" {
			t.Fatalf("unexpected broker addr: %s", addr)
		}

		if req.Group != "g" || req.Topic != "demo" || req.Partition != 0 {
			t.Fatalf("unexpected request: %+v", req)
		}

		return &netproto.FetchCommittedResponse{Error: api.ErrNone, Offset: 22}, nil
	}

	err := runCLIWithContext([]string{"fetch-committed", "-json", "-broker", "broker:7912", "-group", "g", "-topic", "demo"}, ctx)
	if err != nil {
		t.Fatalf("runCLIWithContext() err = %v", err)
	}

	var got fetchCommittedJSONResponse
	decodeJSON(t, out.String(), &got)

	if !got.OK || got.Offset != 22 {
		t.Fatalf("unexpected fetch-committed json: %+v", got)
	}
}

func TestRunCLIPingErrorLeavesStdoutEmpty(t *testing.T) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	ctx := newStubCommandContext(&out, &errOut)
	ctx.ping = func(string) (*netproto.PingResponse, error) {
		return nil, errors.New("dial failed")
	}

	err := runCLIWithContext([]string{"ping", "-json"}, ctx)
	if err == nil {
		t.Fatal("expected ping error")
	}

	if got := out.String(); got != "" {
		t.Fatalf("stdout must stay empty on error, got %q", got)
	}

	if !strings.Contains(err.Error(), "dial failed") {
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

func newStubCommandContext(stdout, stderr io.Writer) *commandContext {
	ctx := newCommandContext(stdout, stderr)
	ctx.now = stubNow(time.Unix(0, 0))

	return ctx
}

func stubNow(values ...time.Time) func() time.Time {
	if len(values) == 0 {
		values = []time.Time{time.Unix(0, 0)}
	}

	index := 0

	return func() time.Time {
		if index >= len(values) {
			return values[len(values)-1]
		}

		value := values[index]
		index++

		return value
	}
}

func decodeJSON(t *testing.T, data string, target any) {
	t.Helper()

	if err := json.Unmarshal([]byte(data), target); err != nil {
		t.Fatalf("json.Unmarshal(%q): %v", data, err)
	}
}
