package netproto

import "github.com/c4erries/wave-mq/pkg/api"

// CreateTopicRequest represents a create-topic command payload.
type CreateTopicRequest struct {
	Topic             string
	Partitions        int
	ReplicationFactor int
}

// CreateTopicResponse acknowledges topic creation.
type CreateTopicResponse struct {
	Error api.ErrorCode
}

// ProduceRequest carries a batch of records for a specific partition.
type ProduceRequest struct {
	Topic     string
	Partition int
	Records   []api.Record
}

// ProduceResponse contains the first offset assigned to the batch.
type ProduceResponse struct {
	BaseOffset api.Offset
	Error      api.ErrorCode
}

// ProduceByKeyRequest carries a batch of records routed by the broker using key hashing.
type ProduceByKeyRequest struct {
	Topic   string
	Key     []byte
	Records []api.Record
}

// ProduceByKeyResponse contains the chosen partition and the first offset assigned to the batch.
type ProduceByKeyResponse struct {
	Partition  int
	BaseOffset api.Offset
	Error      api.ErrorCode
}

// FetchRequest pulls messages starting from Offset up to MaxBytes.
type FetchRequest struct {
	Topic     string
	Partition int
	Offset    api.Offset
	MaxBytes  int32
}

// FetchResponse returns a slice of records plus metadata.
type FetchResponse struct {
	Records []api.Record
	// HighWatermark is the leader's durable offset boundary for the partition.
	HighWatermark api.Offset
	Error         api.ErrorCode
}

// ListOffsetsRequest placeholder (not yet used on the wire).
type ListOffsetsRequest struct {
	Topic     string
	Partition int
}

// ListOffsetsResponse placeholder (not yet used on the wire).
type ListOffsetsResponse struct {
	Earliest api.Offset
	Latest   api.Offset
	Error    api.ErrorCode
}

// MetadataRequest enumerates topics to fetch metadata for.
type MetadataRequest struct {
	Topics []string
}

// MetadataResponse returns partition metadata for requested topics.
type MetadataResponse struct {
	Partitions []api.PartitionMetadata
	Error      api.ErrorCode
}

// CommitOffsetRequest stores a consumer group offset.
type CommitOffsetRequest struct {
	Group     string
	Topic     string
	Partition int
	Offset    api.Offset
}

// CommitOffsetResponse acknowledges the commit.
type CommitOffsetResponse struct {
	Error api.ErrorCode
}

// FetchCommittedRequest asks for the last committed offset.
type FetchCommittedRequest struct {
	Group     string
	Topic     string
	Partition int
}

// FetchCommittedResponse returns a committed offset for a partition.
type FetchCommittedResponse struct {
	Offset api.Offset
	Error  api.ErrorCode
}

// PingRequest is a lightweight liveness probe.
type PingRequest struct{}

// PingResponse echoes status.
type PingResponse struct {
	Error api.ErrorCode
}
