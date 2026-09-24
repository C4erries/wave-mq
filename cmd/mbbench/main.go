package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/pkg/api"
)

type stats struct {
	mu       sync.Mutex
	count    int
	totalDur time.Duration
	maxDur   time.Duration
}

type benchConfig struct {
	brokerAddr  string
	topic       string
	partition   int
	messages    int
	valueSize   int
	concurrency int
}

func (s *stats) add(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.count++

	s.totalDur += d
	if d > s.maxDur {
		s.maxDur = d
	}
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		log.Fatalf("parse flags: %v", err)
	}

	if err := runBenchmark(cfg); err != nil {
		log.Fatalf("benchmark failed: %v", err)
	}
}

func parseFlags(args []string) (benchConfig, error) {
	fs := flag.NewFlagSet("mbbench", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	brokerAddr := fs.String("broker", "127.0.0.1:7912", "binary protocol address")
	topic := fs.String("topic", "bench", "topic name")
	partition := fs.Int("partition", 0, "partition id")
	messages := fs.Int("messages", 10000, "number of messages")
	valueSize := fs.Int("value-size", 100, "payload size in bytes")
	concurrency := fs.Int("concurrency", 4, "producer goroutines")

	if err := fs.Parse(args); err != nil {
		return benchConfig{}, err
	}

	cfg := benchConfig{
		brokerAddr:  *brokerAddr,
		topic:       *topic,
		partition:   *partition,
		messages:    *messages,
		valueSize:   *valueSize,
		concurrency: *concurrency,
	}

	if err := validateBenchConfig(cfg); err != nil {
		return benchConfig{}, err
	}

	return cfg, nil
}

func validateBenchConfig(cfg benchConfig) error {
	switch {
	case cfg.brokerAddr == "":
		return fmt.Errorf("broker is required")
	case cfg.topic == "":
		return fmt.Errorf("topic is required")
	case cfg.partition < 0:
		return fmt.Errorf("partition must be >= 0")
	case cfg.messages <= 0:
		return fmt.Errorf("messages must be > 0")
	case cfg.valueSize <= 0:
		return fmt.Errorf("value-size must be > 0")
	case cfg.concurrency <= 0:
		return fmt.Errorf("concurrency must be > 0")
	default:
		return nil
	}
}

func runBenchmark(cfg benchConfig) error {
	// Ensure topic exists
	if err := createTopic(cfg.brokerAddr, cfg.topic, 1); err != nil {
		log.Printf("create-topic warning: %v", err)
	}

	payload := make([]byte, cfg.valueSize)
	work := buildWorkQueue(cfg.messages)

	var wg sync.WaitGroup

	st := stats{}

	start := time.Now()

	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)

		go runProducer(&wg, &st, cfg, payload, work)
	}

	wg.Wait()
	printStats(&st, cfg.concurrency, time.Since(start))

	return nil
}

func buildWorkQueue(messages int) <-chan struct{} {
	work := make(chan struct{}, messages)
	for i := 0; i < messages; i++ {
		work <- struct{}{}
	}

	close(work)

	return work
}

func runProducer(wg *sync.WaitGroup, st *stats, cfg benchConfig, payload []byte, work <-chan struct{}) {
	defer wg.Done()

	conn, err := net.Dial("tcp", cfg.brokerAddr)
	if err != nil {
		log.Printf("dial: %v", err)
		return
	}
	defer closeConnWithLog(conn)

	for range work {
		req := &netproto.ProduceRequest{
			Topic:     cfg.topic,
			Partition: cfg.partition,
			Records:   []api.Record{{Value: payload}},
		}

		payloadBytes, err := netproto.EncodeProduceRequest(req)
		if err != nil {
			log.Printf("encode produce request: %v", err)
			return
		}

		frame, err := netproto.EncodeRequestFrame(api.APIKeyProduce, 1, payloadBytes)
		if err != nil {
			log.Printf("encode produce frame: %v", err)
			return
		}

		startOp := time.Now()

		if _, err := conn.Write(frame); err != nil {
			log.Printf("write: %v", err)
			return
		}

		_, _, respPayload, err := netproto.DecodeResponseFrame(conn)
		if err != nil {
			log.Printf("read: %v", err)
			return
		}

		resp, err := netproto.DecodeProduceResponse(respPayload)
		if err != nil || resp.Error != api.ErrNone {
			log.Printf("produce resp err=%v code=%v", err, resp.Error)
			return
		}

		st.add(time.Since(startOp))
	}
}

func printStats(st *stats, concurrency int, total time.Duration) {
	if st.count == 0 {
		log.Printf("no messages produced")
		return
	}

	avg := st.totalDur / time.Duration(st.count)
	rps := float64(st.count) / total.Seconds()
	fmt.Printf("produced=%d concurrency=%d avg_latency=%s max_latency=%s total=%s rps=%.0f\n",
		st.count, concurrency, avg, st.maxDur, total, rps)
}

func createTopic(addr, topic string, partitions int) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer closeConnWithLog(conn)

	req := &netproto.CreateTopicRequest{Topic: topic, Partitions: partitions, ReplicationFactor: 1}

	payload, err := netproto.EncodeCreateTopicRequest(req)
	if err != nil {
		return err
	}

	frame, err := netproto.EncodeRequestFrame(api.APIKeyCreateTopic, 1, payload)
	if err != nil {
		return err
	}

	if _, err := conn.Write(frame); err != nil {
		return err
	}

	_, _, respPayload, err := netproto.DecodeResponseFrame(conn)
	if err != nil {
		return err
	}

	resp, err := netproto.DecodeCreateTopicResponse(respPayload)
	if err != nil {
		return err
	}

	if resp.Error != api.ErrNone {
		return fmt.Errorf("create topic error %v", resp.Error)
	}

	return nil
}

func closeConnWithLog(conn net.Conn) {
	if err := conn.Close(); err != nil {
		log.Printf("close connection: %v", err)
	}
}
