package replication

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/pkg/api"
)

type fakeBroker struct {
	data map[string]map[int][]api.Record
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{data: make(map[string]map[int][]api.Record)}
}

func (f *fakeBroker) CreateTopic(ctx context.Context, name string, cfg api.TopicConfig) error {
	_ = ctx
	if _, ok := f.data[name]; ok {
		return nil
	}
	f.data[name] = make(map[int][]api.Record, cfg.Partitions)
	for p := 0; p < cfg.Partitions; p++ {
		f.data[name][p] = []api.Record{}
	}
	return nil
}

func (f *fakeBroker) Produce(ctx context.Context, topic string, partition int, records []api.Record) (api.Offset, error) {
	_ = ctx
	topicParts, ok := f.data[topic]
	if !ok {
		return -1, errors.New("topic not found")
	}
	recs, ok := topicParts[partition]
	if !ok {
		return -1, errors.New("partition not found")
	}
	base := api.Offset(len(recs))
	for i := range records {
		records[i].Offset = base + api.Offset(i)
		recs = append(recs, records[i])
	}
	topicParts[partition] = recs
	return base, nil
}

func (f *fakeBroker) Fetch(ctx context.Context, topic string, partition int, offset api.Offset, maxBytes int32) ([]api.Record, error) {
	_ = ctx
	topicParts, ok := f.data[topic]
	if !ok {
		return nil, errors.New("topic not found")
	}
	recs, ok := topicParts[partition]
	if !ok {
		return nil, errors.New("partition not found")
	}
	if offset < 0 || int(offset) > len(recs) {
		return []api.Record{}, nil
	}
	res := recs[offset:]
	if maxBytes > 0 {
		var total int32
		var trimmed []api.Record
		for _, r := range res {
			total += int32(len(r.Value))
			trimmed = append(trimmed, r)
			if total >= maxBytes {
				break
			}
		}
		res = trimmed
	}
	return res, nil
}

func (f *fakeBroker) ListOffsets(ctx context.Context, topic string, partition int) (api.Offset, api.Offset, error) {
	_ = ctx
	topicParts, ok := f.data[topic]
	if !ok {
		return -1, -1, errors.New("topic not found")
	}
	recs, ok := topicParts[partition]
	if !ok {
		return -1, -1, errors.New("partition not found")
	}
	if len(recs) == 0 {
		return 0, -1, nil
	}
	return 0, api.Offset(len(recs) - 1), nil
}

func (f *fakeBroker) CommitOffset(ctx context.Context, group string, topic string, partition int, offset api.Offset) error {
	_ = ctx
	_ = group
	_ = topic
	_ = partition
	_ = offset
	return nil
}

func (f *fakeBroker) FetchCommitted(ctx context.Context, group string, topic string, partition int) (api.Offset, error) {
	return -1, errors.New("not implemented")
}

func (f *fakeBroker) Metadata(ctx context.Context, topics []string) ([]api.PartitionMetadata, error) {
	_ = ctx
	_ = topics
	return nil, nil
}

func TestBinaryReplicatorFetchSuccess(t *testing.T) {
	b := newFakeBroker()
	if err := b.CreateTopic(context.Background(), "alpha", api.TopicConfig{Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	_, _ = b.Produce(context.Background(), "alpha", 0, []api.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
	})

	srv, err := netproto.NewServer("127.0.0.1:0", b)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = srv.ListenAndServe(ctx)
	}()
	addr := waitForAddr(t, srv)

	rep := NewBinaryReplicator()
	res, err := rep.FetchFromLeader(context.Background(), api.BrokerInfo{Host: addr.String()}, FetchRequest{
		Topic:     "alpha",
		Partition: 0,
		Offset:    0,
		MaxBytes:  1024,
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(res.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(res.Records))
	}
	if string(res.Records[0].Value) != "one" || string(res.Records[1].Value) != "two" {
		t.Fatalf("unexpected payloads: %+v", res.Records)
	}
	if res.HighWatermark != 1 {
		t.Fatalf("expected hwm 1, got %d", res.HighWatermark)
	}
}

func TestBinaryReplicatorConnectionFailure(t *testing.T) {
	rep := NewBinaryReplicator()
	_, err := rep.FetchFromLeader(context.Background(), api.BrokerInfo{Host: "127.0.0.1:1"}, FetchRequest{
		Topic:     "missing",
		Partition: 0,
		Offset:    0,
		MaxBytes:  1024,
	})
	if err == nil {
		t.Fatalf("expected dial error")
	}
}

func waitForAddr(t *testing.T, srv *netproto.Server) net.Addr {
	t.Helper()
	for i := 0; i < 50; i++ {
		if addr := srv.Addr(); addr != nil {
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not start listening in time")
	return nil
}
