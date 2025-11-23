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

func sendCreateTopic(addr string, req *netproto.CreateTopicRequest) (*netproto.CreateTopicResponse, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	payload, err := encodeWithCorr(api.APIKeyCreateTopic, req, conn)
	if err != nil {
		return nil, err
	}
	return netproto.DecodeCreateTopicResponse(payload)
}

func sendProduce(addr string, req *netproto.ProduceRequest) (*netproto.ProduceResponse, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	payload, err := encodeWithCorr(api.APIKeyProduce, req, conn)
	if err != nil {
		return nil, err
	}
	return netproto.DecodeProduceResponse(payload)
}

func sendFetch(addr string, req *netproto.FetchRequest) (*netproto.FetchResponse, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	payload, err := encodeWithCorr(api.APIKeyFetch, req, conn)
	if err != nil {
		return nil, err
	}
	return netproto.DecodeFetchResponse(payload)
}

func encodeWithCorr(apiKey api.APIKey, req interface{}, conn net.Conn) ([]byte, error) {
	var (
		payload []byte
		err     error
	)
	switch v := req.(type) {
	case *netproto.CreateTopicRequest:
		payload, err = netproto.EncodeCreateTopicRequest(v)
	case *netproto.ProduceRequest:
		payload, err = netproto.EncodeProduceRequest(v)
	case *netproto.FetchRequest:
		payload, err = netproto.EncodeFetchRequest(v)
	default:
		return nil, fmt.Errorf("unsupported request type")
	}
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
