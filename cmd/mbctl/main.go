package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/pkg/api"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	switch os.Args[1] {
	case "create-topic":
		handleCreateTopic(os.Args[2:])
	case "produce":
		handleProduce(os.Args[2:])
	case "fetch":
		handleFetch(os.Args[2:])
	case "metadata":
		handleMetadata(os.Args[2:])
	case "list-offsets":
		handleListOffsets(os.Args[2:])
	case "commit-offset":
		handleCommitOffset(os.Args[2:])
	case "fetch-committed":
		handleFetchCommitted(os.Args[2:])
	case "ping":
		handlePing(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: mbctl <command> [options]
Commands:
  create-topic   Create a topic with partitions/replication
  produce        Produce one or more messages
  fetch          Fetch messages from a partition
  metadata       Get metadata for topics
  list-offsets   Get earliest/latest offsets for a partition
  commit-offset  Commit offset for a consumer group
  fetch-committed Fetch committed offset for a consumer group
  ping           Ping broker
`)
}

func handleCreateTopic(args []string) {
	fs := flag.NewFlagSet("create-topic", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partitions := fs.Int("partitions", 1, "number of partitions")
	replication := fs.Int("replication-factor", 1, "replication factor")
	_ = fs.Parse(args)

	req := &netproto.CreateTopicRequest{
		Topic:             *topic,
		Partitions:        *partitions,
		ReplicationFactor: *replication,
	}
	if req.Topic == "" {
		fmt.Fprintln(os.Stderr, "topic is required")
		os.Exit(1)
	}
	resp, err := sendCreateTopic(*brokerAddr, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create-topic error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "create-topic failed: %v\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("topic %s created (partitions=%d rf=%d)\n", req.Topic, req.Partitions, req.ReplicationFactor)
}

func handleProduce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	key := fs.String("key", "", "record key (optional)")
	value := fs.String("value", "", "record value")
	_ = fs.Parse(args)

	req := &netproto.ProduceRequest{
		Topic:     *topic,
		Partition: *partition,
		Records: []api.Record{{
			Key:       []byte(*key),
			Value:     []byte(*value),
			Timestamp: time.Now(),
		}},
	}
	if req.Topic == "" {
		fmt.Fprintln(os.Stderr, "topic is required")
		os.Exit(1)
	}
	resp, err := sendProduce(*brokerAddr, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "produce error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "produce failed: %v\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("produced baseOffset=%d\n", resp.BaseOffset)
}

func handleFetch(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	offset := fs.Int64("offset", 0, "starting offset")
	maxBytes := fs.Int("max-bytes", 1<<20, "max bytes to fetch")
	_ = fs.Parse(args)

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "topic is required")
		os.Exit(1)
	}
	req := &netproto.FetchRequest{
		Topic:     *topic,
		Partition: *partition,
		Offset:    api.Offset(*offset),
		MaxBytes:  int32(*maxBytes),
	}
	resp, err := sendFetch(*brokerAddr, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "fetch failed: %v\n", resp.Error)
		os.Exit(1)
	}
	for _, r := range resp.Records {
		fmt.Printf("offset=%d key=%s value=%s\n", r.Offset, string(r.Key), string(r.Value))
	}
}

func handleMetadata(args []string) {
	fs := flag.NewFlagSet("metadata", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name (optional)")
	_ = fs.Parse(args)

	req := &netproto.MetadataRequest{}
	if *topic != "" {
		req.Topics = []string{*topic}
	}
	resp, err := sendMetadata(*brokerAddr, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "metadata error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "metadata failed: %v\n", resp.Error)
		os.Exit(1)
	}
	for _, p := range resp.Partitions {
		fmt.Printf("topic=%s partition=%d broker=%d role=%d epoch=%d start=%d hwm=%d\n",
			p.Replica.Topic, p.Replica.Partition, p.Replica.BrokerID, p.Replica.Role, p.Replica.LeaderEpoch, p.StartOffset, p.HighWatermark)
	}
}

func handleListOffsets(args []string) {
	fs := flag.NewFlagSet("list-offsets", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	_ = fs.Parse(args)

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "topic is required")
		os.Exit(1)
	}
	req := &netproto.ListOffsetsRequest{Topic: *topic, Partition: *partition}
	resp, err := sendListOffsets(*brokerAddr, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list-offsets error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "list-offsets failed: %v\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("earliest=%d latest=%d\n", resp.Earliest, resp.Latest)
}

func handleCommitOffset(args []string) {
	fs := flag.NewFlagSet("commit-offset", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	group := fs.String("group", "", "consumer group")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	offset := fs.Int64("offset", 0, "offset to commit")
	_ = fs.Parse(args)
	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "group and topic are required")
		os.Exit(1)
	}
	req := &netproto.CommitOffsetRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: *partition,
		Offset:    api.Offset(*offset),
	}
	resp, err := sendCommitOffset(*brokerAddr, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "commit-offset error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "commit-offset failed: %v\n", resp.Error)
		os.Exit(1)
	}
	fmt.Println("commit-offset ok")
}

func handleFetchCommitted(args []string) {
	fs := flag.NewFlagSet("fetch-committed", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	group := fs.String("group", "", "consumer group")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	_ = fs.Parse(args)
	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "group and topic are required")
		os.Exit(1)
	}
	req := &netproto.FetchCommittedRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: *partition,
	}
	resp, err := sendFetchCommitted(*brokerAddr, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch-committed error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "fetch-committed failed: %v\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("committed offset=%d\n", resp.Offset)
}

func handlePing(args []string) {
	fs := flag.NewFlagSet("ping", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	_ = fs.Parse(args)
	start := time.Now()
	resp, err := sendPing(*brokerAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ping error: %v\n", err)
		os.Exit(1)
	}
	if resp.Error != api.ErrNone {
		fmt.Fprintf(os.Stderr, "ping failed: %v\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("pong rtt=%s\n", time.Since(start))
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
