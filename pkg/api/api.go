package api

import "time"

// Offset is a logical position inside a partition log.
// It starts from 0 and increases by one per record.
type Offset int64

// PartitionRole describes leader/follower roles for a partition replica.
type PartitionRole int

const (
	// RoleLeader owns the writable replica for a partition.
	RoleLeader PartitionRole = iota
	// RoleFollower mirrors a leader; unused in the single-node MVP but reserved.
	RoleFollower
)

// BrokerConfig holds process-wide configuration including cluster-ready fields.
// In the current MVP BrokerID=1 and ReplicationFactor=1 are expected defaults.
type BrokerConfig struct {
	BrokerID          int
	ReplicationFactor int
	DataDir           string
	BinaryAddr        string
	MQTTAddr          string
	HTTPAddr          string
	MaxSegmentBytes   int64
	RetentionBytes    int64
	RetentionTime     time.Duration
	ClusterID         string
	ControllerAddr    string
	AdvertisedAddr    string
	StaticCluster     *StaticClusterConfig
	ControllerMode    string // "single" (default) or "raft"
	RaftDir           string
	RaftBindAddr      string
	RaftPeers         []string
	Replication       bool // enable follower replication manager
}

// TopicConfig describes how a topic should be created.
type TopicConfig struct {
	Partitions        int
	ReplicationFactor int
}

// PartitionReplica identifies a concrete partition replica on a broker.
type PartitionReplica struct {
	Topic       string
	Partition   int
	BrokerID    int
	Role        PartitionRole
	LeaderEpoch int32
}

// PartitionMetadata captures per-partition state that is exposed to clients.
type PartitionMetadata struct {
	Replica       PartitionReplica
	StartOffset   Offset
	HighWatermark Offset
}

// BrokerInfo describes a broker in the cluster.
type BrokerInfo struct {
	BrokerID int
	Host     string
	Port     int
	Rack     string
}

// PartitionAssignment describes replica layout and leader/ISR for a partition.
type PartitionAssignment struct {
	Topic       string
	Partition   int
	Replicas    []int
	ISR         []int
	Leader      int
	LeaderEpoch int32
}

// ClusterMetadata captures cluster-wide broker and partition state.
type ClusterMetadata struct {
	ClusterID  string
	Version    int64
	Brokers    []BrokerInfo
	Partitions []PartitionAssignment
}

// StaticClusterConfig describes a preconfigured cluster layout used for bootstrapping.
// It is intended for early multi-broker experiments before dynamic controllers/consensus.
type StaticClusterConfig struct {
	ClusterID string
	Brokers   []BrokerInfo
}

// Header is an optional key/value pair attached to a record.
type Header struct {
	Key   string
	Value []byte
}

// Record is the logical message stored in the commit log.
type Record struct {
	Offset    Offset
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
	CRC32C    uint32
}

// APIKey identifies a request type in the binary protocol.
type APIKey int16

const (
	APIKeyCreateTopic APIKey = iota
	APIKeyProduce
	APIKeyFetch
	APIKeyListOffsets
	APIKeyCommitOffset
	APIKeyFetchCommitted
	APIKeyMetadata
	APIKeyPing
)

// ErrorCode is a lightweight numeric error for the binary protocol.
type ErrorCode int16

const (
	ErrNone ErrorCode = iota
	ErrUnknown
	ErrInvalidRequest
	ErrTopicNotFound
	ErrPartitionNotFound
	ErrTopicExists
	ErrNotLeader
	ErrInternal
	// TODO: extend with protocol-compatible error codes as features land.
)
