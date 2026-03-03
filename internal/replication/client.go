package replication

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/c4erries/wave-mq/internal/netproto"
	"github.com/c4erries/wave-mq/pkg/api"
)

// BinaryReplicator uses the existing binary protocol to fetch records from a leader.
type BinaryReplicator struct {
	DialTimeout time.Duration
}

// NewBinaryReplicator returns a replicator with sane defaults.
func NewBinaryReplicator() *BinaryReplicator {
	return &BinaryReplicator{
		DialTimeout: 5 * time.Second,
	}
}

// FetchFromLeader connects to the leader broker and issues a Fetch request via the binary protocol.
func (r *BinaryReplicator) FetchFromLeader(ctx context.Context, leader api.BrokerInfo, req FetchRequest) (FetchResponse, error) {
	var resp FetchResponse

	addr, err := leaderAddress(leader)
	if err != nil {
		return resp, err
	}

	dialer := &net.Dialer{Timeout: r.DialTimeout}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return resp, err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return resp, err
		}
	}

	payload, err := netproto.EncodeFetchRequest(&netproto.FetchRequest{
		Topic:     req.Topic,
		Partition: req.Partition,
		Offset:    req.Offset,
		MaxBytes:  req.MaxBytes,
	})
	if err != nil {
		return resp, err
	}

	frame, err := netproto.EncodeRequestFrame(api.APIKeyFetch, 1, payload)
	if err != nil {
		return resp, err
	}

	if _, err := conn.Write(frame); err != nil {
		return resp, err
	}

	apiKey, _, payloadResp, err := netproto.DecodeResponseFrame(conn)
	if err != nil {
		return resp, err
	}

	if apiKey != api.APIKeyFetch {
		return resp, fmt.Errorf("unexpected api key %d in response", apiKey)
	}

	fr, err := netproto.DecodeFetchResponse(payloadResp)
	if err != nil {
		return resp, err
	}

	resp.Error = fr.Error
	if fr.Error != api.ErrNone {
		return resp, fmt.Errorf("leader returned %d", fr.Error)
	}

	resp.Records = fr.Records
	resp.HighWatermark = fr.HighWatermark

	return resp, nil
}

func leaderAddress(leader api.BrokerInfo) (string, error) {
	host := leader.Host
	if host == "" {
		return "", fmt.Errorf("leader host is empty")
	}

	if leader.Port != 0 {
		if _, _, err := net.SplitHostPort(host); err != nil {
			return net.JoinHostPort(host, strconv.Itoa(leader.Port)), nil
		}
	}

	return host, nil
}
