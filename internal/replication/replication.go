package replication

import (
	"context"

	"github.com/c4erries/wave-mq/pkg/api"
)

// FetchRequest represents a follower's fetch from a leader replica.
type FetchRequest struct {
	Topic     string
	Partition int
	Offset    api.Offset
	MaxBytes  int32
}

// FetchResponse carries records and the leader's high watermark.
type FetchResponse struct {
	Records       []api.Record
	HighWatermark api.Offset
	Error         api.ErrorCode
}

// Replicator abstracts leader-to-follower replication.
// The implementation is expected to reuse the existing binary protocol or a subset of it.
type Replicator interface {
	FetchFromLeader(ctx context.Context, leader api.BrokerInfo, req FetchRequest) (FetchResponse, error)
}

// TODO: implement replication client/server using the binary protocol for broker-to-broker traffic.
