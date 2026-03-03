package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/pkg/api"
)

const maxInt32 = int(^uint32(0) >> 1)

type command struct {
	name        string
	description string
	run         func(*commandContext, []string) error
}

type commandContext struct {
	stdout io.Writer
	stderr io.Writer
	now    func() time.Time
}

var commandList = []command{
	{name: "create-topic", description: "Create a topic with partitions/replication", run: runCreateTopic},
	{name: "produce", description: "Produce one or more messages", run: runProduce},
	{name: "fetch", description: "Fetch messages from a partition", run: runFetch},
	{name: "metadata", description: "Get metadata for topics", run: runMetadata},
	{name: "list-offsets", description: "Get earliest/latest offsets for a partition", run: runListOffsets},
	{name: "commit-offset", description: "Commit offset for a consumer group", run: runCommitOffset},
	{name: "fetch-committed", description: "Fetch committed offset for a consumer group", run: runFetchCommitted},
	{name: "ping", description: "Ping broker", run: runPing},
}

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	ctx := &commandContext{stdout: stdout, stderr: stderr, now: time.Now}
	if len(args) == 0 {
		usage(stderr)
		return nil
	}

	switch args[0] {
	case "help", "-h", "--help":
		usage(stderr)
		return nil
	}

	cmd, ok := findCommand(args[0])
	if !ok {
		usage(stderr)
		return fmt.Errorf("unknown command: %q", args[0])
	}

	if err := cmd.run(ctx, args[1:]); err != nil {
		return fmt.Errorf("%s: %w", cmd.name, err)
	}

	return nil
}

func findCommand(name string) (command, bool) {
	for _, cmd := range commandList {
		if cmd.name == name {
			return cmd, true
		}
	}

	return command{}, false
}

func usage(w io.Writer) {
	var b strings.Builder
	b.WriteString("Usage: mbctl <command> [options]\n")
	b.WriteString("Commands:\n")
	for _, cmd := range commandList {
		fmt.Fprintf(&b, "  %-14s %s\n", cmd.name, cmd.description)
	}

	fmt.Fprint(w, b.String())
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func runCreateTopic(ctx *commandContext, args []string) error {
	fs := newFlagSet("create-topic", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partitions := fs.Int("partitions", 1, "number of partitions")
	replication := fs.Int("replication-factor", 1, "replication factor")
	if err := fs.Parse(args); err != nil {
		return err
	}

	req := &netproto.CreateTopicRequest{
		Topic:             *topic,
		Partitions:        *partitions,
		ReplicationFactor: *replication,
	}
	if req.Topic == "" {
		return errors.New("topic is required")
	}

	resp, err := sendCreateTopic(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	fmt.Fprintf(ctx.stdout, "topic %s created (partitions=%d rf=%d)\n", req.Topic, req.Partitions, req.ReplicationFactor)
	return nil
}

func runProduce(ctx *commandContext, args []string) error {
	fs := newFlagSet("produce", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	key := fs.String("key", "", "record key (optional)")
	value := fs.String("value", "", "record value")
	if err := fs.Parse(args); err != nil {
		return err
	}

	req := &netproto.ProduceRequest{
		Topic:     *topic,
		Partition: *partition,
		Records: []api.Record{{
			Key:       []byte(*key),
			Value:     []byte(*value),
			Timestamp: ctx.now(),
		}},
	}
	if req.Topic == "" {
		return errors.New("topic is required")
	}

	resp, err := sendProduce(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	fmt.Fprintf(ctx.stdout, "produced baseOffset=%d\n", resp.BaseOffset)
	return nil
}

func runFetch(ctx *commandContext, args []string) error {
	fs := newFlagSet("fetch", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	offset := fs.Int64("offset", 0, "starting offset")
	maxBytes := fs.Int("max-bytes", 1<<20, "max bytes to fetch")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *topic == "" {
		return errors.New("topic is required")
	}

	maxFetchBytes, err := toInt32(*maxBytes, "max-bytes")
	if err != nil {
		return err
	}

	req := &netproto.FetchRequest{
		Topic:     *topic,
		Partition: *partition,
		Offset:    api.Offset(*offset),
		MaxBytes:  maxFetchBytes,
	}

	resp, err := sendFetch(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	for _, r := range resp.Records {
		fmt.Fprintf(ctx.stdout, "offset=%d key=%s value=%s\n", r.Offset, string(r.Key), string(r.Value))
	}

	return nil
}

func runMetadata(ctx *commandContext, args []string) error {
	fs := newFlagSet("metadata", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	req := &netproto.MetadataRequest{}
	if *topic != "" {
		req.Topics = []string{*topic}
	}

	resp, err := sendMetadata(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	for _, p := range resp.Partitions {
		fmt.Fprintf(ctx.stdout, "topic=%s partition=%d broker=%d role=%d epoch=%d start=%d hwm=%d\n",
			p.Replica.Topic, p.Replica.Partition, p.Replica.BrokerID, p.Replica.Role, p.Replica.LeaderEpoch, p.StartOffset, p.HighWatermark)
	}

	return nil
}

func runListOffsets(ctx *commandContext, args []string) error {
	fs := newFlagSet("list-offsets", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *topic == "" {
		return errors.New("topic is required")
	}

	req := &netproto.ListOffsetsRequest{Topic: *topic, Partition: *partition}

	resp, err := sendListOffsets(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	fmt.Fprintf(ctx.stdout, "earliest=%d latest=%d\n", resp.Earliest, resp.Latest)
	return nil
}

func runCommitOffset(ctx *commandContext, args []string) error {
	fs := newFlagSet("commit-offset", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	group := fs.String("group", "", "consumer group")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	offset := fs.Int64("offset", 0, "offset to commit")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *group == "" || *topic == "" {
		return errors.New("group and topic are required")
	}

	req := &netproto.CommitOffsetRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: *partition,
		Offset:    api.Offset(*offset),
	}

	resp, err := sendCommitOffset(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	fmt.Fprintln(ctx.stdout, "commit-offset ok")
	return nil
}

func runFetchCommitted(ctx *commandContext, args []string) error {
	fs := newFlagSet("fetch-committed", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	group := fs.String("group", "", "consumer group")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *group == "" || *topic == "" {
		return errors.New("group and topic are required")
	}

	req := &netproto.FetchCommittedRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: *partition,
	}

	resp, err := sendFetchCommitted(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	fmt.Fprintf(ctx.stdout, "committed offset=%d\n", resp.Offset)
	return nil
}

func runPing(ctx *commandContext, args []string) error {
	fs := newFlagSet("ping", ctx.stderr)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	if err := fs.Parse(args); err != nil {
		return err
	}

	start := ctx.now()
	resp, err := sendPing(*brokerAddr)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	fmt.Fprintf(ctx.stdout, "pong rtt=%s\n", ctx.now().Sub(start))
	return nil
}

func sendCreateTopic(addr string, req *netproto.CreateTopicRequest) (*netproto.CreateTopicResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyCreateTopic, func() ([]byte, error) {
		return netproto.EncodeCreateTopicRequest(req)
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodeCreateTopicResponse(respPayload)
}

func sendProduce(addr string, req *netproto.ProduceRequest) (*netproto.ProduceResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyProduce, func() ([]byte, error) {
		return netproto.EncodeProduceRequest(req)
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodeProduceResponse(respPayload)
}

func sendFetch(addr string, req *netproto.FetchRequest) (*netproto.FetchResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyFetch, func() ([]byte, error) {
		return netproto.EncodeFetchRequest(req)
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodeFetchResponse(respPayload)
}

func sendMetadata(addr string, req *netproto.MetadataRequest) (*netproto.MetadataResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyMetadata, func() ([]byte, error) {
		return netproto.EncodeMetadataRequest(req)
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodeMetadataResponse(respPayload)
}

func sendListOffsets(addr string, req *netproto.ListOffsetsRequest) (*netproto.ListOffsetsResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyListOffsets, func() ([]byte, error) {
		return netproto.EncodeListOffsetsRequest(req)
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodeListOffsetsResponse(respPayload)
}

func sendCommitOffset(addr string, req *netproto.CommitOffsetRequest) (*netproto.CommitOffsetResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyCommitOffset, func() ([]byte, error) {
		return netproto.EncodeCommitOffsetRequest(req)
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodeCommitOffsetResponse(respPayload)
}

func sendFetchCommitted(addr string, req *netproto.FetchCommittedRequest) (*netproto.FetchCommittedResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyFetchCommitted, func() ([]byte, error) {
		return netproto.EncodeFetchCommittedRequest(req)
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodeFetchCommittedResponse(respPayload)
}

func sendPing(addr string) (*netproto.PingResponse, error) {
	respPayload, err := sendRequest(addr, api.APIKeyPing, func() ([]byte, error) {
		return netproto.EncodePingRequest(&netproto.PingRequest{})
	})
	if err != nil {
		return nil, err
	}

	return netproto.DecodePingResponse(respPayload)
}

func sendRequest(addr string, apiKey api.APIKey, payloadFn func() ([]byte, error)) ([]byte, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	payload, err := payloadFn()
	if err != nil {
		return nil, err
	}

	frame, err := netproto.EncodeRequestFrame(apiKey, 1, payload)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}

	_, _, respPayload, err := netproto.DecodeResponseFrame(conn)

	return respPayload, err
}

func toInt32(v int, field string) (int32, error) {
	if v < 0 || v > maxInt32 {
		return 0, fmt.Errorf("%s out of int32 range: %d", field, v)
	}

	return int32(v), nil // #nosec G115 -- bounds checked above.
}
