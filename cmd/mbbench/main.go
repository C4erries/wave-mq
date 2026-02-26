package main

import (
	"flag"
	"fmt"
	"log"
	"net"
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
	cfg := parseFlags()
	runBenchmark(cfg)
}

func parseFlags() benchConfig {
	brokerAddr := flag.String("broker", "127.0.0.1:7912", "binary protocol address")
	topic := flag.String("topic", "bench", "topic name")
	partition := flag.Int("partition", 0, "partition id")
	messages := flag.Int("messages", 10000, "number of messages")
	valueSize := flag.Int("value-size", 100, "payload size in bytes")
	concurrency := flag.Int("concurrency", 4, "producer goroutines")

	flag.Parse()

	return benchConfig{
		brokerAddr:  *brokerAddr,
		topic:       *topic,
		partition:   *partition,
		messages:    *messages,
		valueSize:   *valueSize,
		concurrency: *concurrency,
	}
}

func runBenchmark(cfg benchConfig) {
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
	defer conn.Close()

	for range work {
		req := &netproto.ProduceRequest{
			Topic:     cfg.topic,
			Partition: cfg.partition,
			Records:   []api.Record{{Value: payload}},
		}
		payloadBytes, _ := netproto.EncodeProduceRequest(req)
		frame, _ := netproto.EncodeRequestFrame(api.APIKeyProduce, 1, payloadBytes)
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
	defer conn.Close()

	req := &netproto.CreateTopicRequest{Topic: topic, Partitions: partitions, ReplicationFactor: 1}
	payload, _ := netproto.EncodeCreateTopicRequest(req)

	frame, _ := netproto.EncodeRequestFrame(api.APIKeyCreateTopic, 1, payload)
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
