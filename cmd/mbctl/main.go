package main

import (
	"encoding/json"
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

const (
	defaultDialTimeout    = 5 * time.Second
	defaultRequestTimeout = 10 * time.Second
)

type command struct {
	name        string
	description string
	run         func(*commandContext, []string) error
}

type commandContext struct {
	stdout io.Writer
	stderr io.Writer
	now    func() time.Time

	createTopic    func(string, *netproto.CreateTopicRequest) (*netproto.CreateTopicResponse, error)
	produce        func(string, *netproto.ProduceRequest) (*netproto.ProduceResponse, error)
	fetch          func(string, *netproto.FetchRequest) (*netproto.FetchResponse, error)
	metadata       func(string, *netproto.MetadataRequest) (*netproto.MetadataResponse, error)
	listOffsets    func(string, *netproto.ListOffsetsRequest) (*netproto.ListOffsetsResponse, error)
	commitOffset   func(string, *netproto.CommitOffsetRequest) (*netproto.CommitOffsetResponse, error)
	fetchCommitted func(string, *netproto.FetchCommittedRequest) (*netproto.FetchCommittedResponse, error)
	ping           func(string) (*netproto.PingResponse, error)
}

type okJSONResponse struct {
	OK bool `json:"ok"`
}

type pingJSONResponse struct {
	OK    bool  `json:"ok"`
	RTTMs int64 `json:"rttMs"`
}

type createTopicJSONResponse struct {
	OK                bool   `json:"ok"`
	Topic             string `json:"topic"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
}

type produceJSONResponse struct {
	OK         bool       `json:"ok"`
	BaseOffset api.Offset `json:"baseOffset"`
}

type fetchRecordJSON struct {
	Offset api.Offset `json:"offset"`
	Key    string     `json:"key"`
	Value  string     `json:"value"`
}

type fetchJSONResponse struct {
	OK            bool              `json:"ok"`
	HighWatermark api.Offset        `json:"highWatermark"`
	Records       []fetchRecordJSON `json:"records"`
}

type metadataPartitionJSON struct {
	Topic         string     `json:"topic"`
	Partition     int        `json:"partition"`
	BrokerID      int        `json:"brokerId"`
	Role          string     `json:"role"`
	LeaderEpoch   int32      `json:"leaderEpoch"`
	StartOffset   api.Offset `json:"startOffset"`
	HighWatermark api.Offset `json:"highWatermark"`
	Leader        int        `json:"leader"`
	Replicas      []int      `json:"replicas"`
	ISR           []int      `json:"isr"`
}

type metadataJSONResponse struct {
	OK         bool                    `json:"ok"`
	Partitions []metadataPartitionJSON `json:"partitions"`
}

type listOffsetsJSONResponse struct {
	OK       bool       `json:"ok"`
	Earliest api.Offset `json:"earliest"`
	Latest   api.Offset `json:"latest"`
}

type fetchCommittedJSONResponse struct {
	OK     bool       `json:"ok"`
	Offset api.Offset `json:"offset"`
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

func newCommandContext(stdout, stderr io.Writer) *commandContext {
	return &commandContext{
		stdout:         stdout,
		stderr:         stderr,
		now:            time.Now,
		createTopic:    sendCreateTopic,
		produce:        sendProduce,
		fetch:          sendFetch,
		metadata:       sendMetadata,
		listOffsets:    sendListOffsets,
		commitOffset:   sendCommitOffset,
		fetchCommitted: sendFetchCommitted,
		ping:           sendPing,
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	return runCLIWithContext(args, newCommandContext(stdout, stderr))
}

func runCLIWithContext(args []string, ctx *commandContext) error {
	if len(args) == 0 {
		usage(ctx.stderr)
		return nil
	}

	switch args[0] {
	case "help", "-h", "--help":
		usage(ctx.stderr)
		return nil
	}

	cmd, ok := findCommand(args[0])
	if !ok {
		usage(ctx.stderr)
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

func addJSONFlag(fs *flag.FlagSet) *bool {
	return fs.Bool("json", false, "emit machine-readable JSON output")
}

func writeJSON(w io.Writer, value any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(value)
}

func partitionRoleName(role api.PartitionRole) string {
	switch role {
	case api.RoleLeader:
		return "leader"
	case api.RoleFollower:
		return "follower"
	default:
		return fmt.Sprintf("unknown(%d)", role)
	}
}

func runCreateTopic(ctx *commandContext, args []string) error {
	fs := newFlagSet("create-topic", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
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

	resp, err := ctx.createTopic(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	if *jsonOutput {
		return writeJSON(ctx.stdout, createTopicJSONResponse{
			OK:                true,
			Topic:             req.Topic,
			Partitions:        req.Partitions,
			ReplicationFactor: req.ReplicationFactor,
		})
	}

	fmt.Fprintf(ctx.stdout, "topic %s created (partitions=%d rf=%d)\n", req.Topic, req.Partitions, req.ReplicationFactor)
	return nil
}

func runProduce(ctx *commandContext, args []string) error {
	fs := newFlagSet("produce", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
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

	resp, err := ctx.produce(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	if *jsonOutput {
		return writeJSON(ctx.stdout, produceJSONResponse{
			OK:         true,
			BaseOffset: resp.BaseOffset,
		})
	}

	fmt.Fprintf(ctx.stdout, "produced baseOffset=%d\n", resp.BaseOffset)
	return nil
}

func runFetch(ctx *commandContext, args []string) error {
	fs := newFlagSet("fetch", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
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

	resp, err := ctx.fetch(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	if *jsonOutput {
		records := make([]fetchRecordJSON, 0, len(resp.Records))
		for _, record := range resp.Records {
			records = append(records, fetchRecordJSON{
				Offset: record.Offset,
				Key:    string(record.Key),
				Value:  string(record.Value),
			})
		}
		return writeJSON(ctx.stdout, fetchJSONResponse{
			OK:            true,
			HighWatermark: resp.HighWatermark,
			Records:       records,
		})
	}

	for _, record := range resp.Records {
		fmt.Fprintf(ctx.stdout, "offset=%d key=%s value=%s\n", record.Offset, string(record.Key), string(record.Value))
	}

	return nil
}

func runMetadata(ctx *commandContext, args []string) error {
	fs := newFlagSet("metadata", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	req := &netproto.MetadataRequest{}
	if *topic != "" {
		req.Topics = []string{*topic}
	}

	resp, err := ctx.metadata(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	if *jsonOutput {
		partitions := make([]metadataPartitionJSON, 0, len(resp.Partitions))
		for _, partitionMeta := range resp.Partitions {
			partitions = append(partitions, metadataPartitionJSON{
				Topic:         partitionMeta.Replica.Topic,
				Partition:     partitionMeta.Replica.Partition,
				BrokerID:      partitionMeta.Replica.BrokerID,
				Role:          partitionRoleName(partitionMeta.Replica.Role),
				LeaderEpoch:   partitionMeta.Replica.LeaderEpoch,
				StartOffset:   partitionMeta.StartOffset,
				HighWatermark: partitionMeta.HighWatermark,
				Leader:        partitionMeta.Leader,
				Replicas:      append([]int(nil), partitionMeta.Replicas...),
				ISR:           append([]int(nil), partitionMeta.ISR...),
			})
		}
		return writeJSON(ctx.stdout, metadataJSONResponse{
			OK:         true,
			Partitions: partitions,
		})
	}

	for _, partitionMeta := range resp.Partitions {
		fmt.Fprintf(
			ctx.stdout,
			"topic=%s partition=%d broker=%d role=%d epoch=%d start=%d hwm=%d\n",
			partitionMeta.Replica.Topic,
			partitionMeta.Replica.Partition,
			partitionMeta.Replica.BrokerID,
			partitionMeta.Replica.Role,
			partitionMeta.Replica.LeaderEpoch,
			partitionMeta.StartOffset,
			partitionMeta.HighWatermark,
		)
	}

	return nil
}

func runListOffsets(ctx *commandContext, args []string) error {
	fs := newFlagSet("list-offsets", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
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
	resp, err := ctx.listOffsets(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	if *jsonOutput {
		return writeJSON(ctx.stdout, listOffsetsJSONResponse{
			OK:       true,
			Earliest: resp.Earliest,
			Latest:   resp.Latest,
		})
	}

	fmt.Fprintf(ctx.stdout, "earliest=%d latest=%d\n", resp.Earliest, resp.Latest)
	return nil
}

func runCommitOffset(ctx *commandContext, args []string) error {
	fs := newFlagSet("commit-offset", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
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

	resp, err := ctx.commitOffset(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	if *jsonOutput {
		return writeJSON(ctx.stdout, okJSONResponse{OK: true})
	}

	fmt.Fprintln(ctx.stdout, "commit-offset ok")
	return nil
}

func runFetchCommitted(ctx *commandContext, args []string) error {
	fs := newFlagSet("fetch-committed", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
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

	resp, err := ctx.fetchCommitted(*brokerAddr, req)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	if *jsonOutput {
		return writeJSON(ctx.stdout, fetchCommittedJSONResponse{
			OK:     true,
			Offset: resp.Offset,
		})
	}

	fmt.Fprintf(ctx.stdout, "committed offset=%d\n", resp.Offset)
	return nil
}

func runPing(ctx *commandContext, args []string) error {
	fs := newFlagSet("ping", ctx.stderr)
	jsonOutput := addJSONFlag(fs)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	if err := fs.Parse(args); err != nil {
		return err
	}

	start := ctx.now()
	resp, err := ctx.ping(*brokerAddr)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("request failed: %v", resp.Error)
	}

	rtt := ctx.now().Sub(start)
	if *jsonOutput {
		return writeJSON(ctx.stdout, pingJSONResponse{
			OK:    true,
			RTTMs: rtt.Milliseconds(),
		})
	}

	fmt.Fprintf(ctx.stdout, "pong rtt=%s\n", rtt)
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

func sendRequest(addr string, apiKey api.APIKey, payloadFn func() ([]byte, error)) (respPayload []byte, err error) {
	conn, err := (&net.Dialer{Timeout: defaultDialTimeout}).Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := conn.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	if err := conn.SetDeadline(time.Now().Add(defaultRequestTimeout)); err != nil {
		return nil, err
	}

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

	_, _, respPayload, err = netproto.DecodeResponseFrame(conn)
	return respPayload, err
}

func toInt32(v int, field string) (int32, error) {
	if v < 0 || v > maxInt32 {
		return 0, fmt.Errorf("%s out of int32 range: %d", field, v)
	}

	return int32(v), nil // #nosec G115 -- bounds checked above.
}
