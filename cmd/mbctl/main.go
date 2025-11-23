package main

import (
	"flag"
	"fmt"
	"os"
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

	fmt.Printf("create-topic against %s topic=%s partitions=%d rf=%d: not implemented\n", *brokerAddr, *topic, *partitions, *replication)
}

func handleProduce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	key := fs.String("key", "", "record key (optional)")
	value := fs.String("value", "", "record value")
	_ = fs.Parse(args)

	fmt.Printf("produce against %s topic=%s partition=%d key=%q value=%q: not implemented\n", *brokerAddr, *topic, *partition, *key, *value)
}

func handleFetch(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address of broker")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	offset := fs.Int64("offset", 0, "starting offset")
	maxBytes := fs.Int("max-bytes", 1<<20, "max bytes to fetch")
	_ = fs.Parse(args)

	fmt.Printf("fetch against %s topic=%s partition=%d offset=%d maxBytes=%d: not implemented\n", *brokerAddr, *topic, *partition, *offset, *maxBytes)
}
