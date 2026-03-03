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
	RetentionBytes    int64
	RetentionTime     time.Duration
}

// PartitionReplica identifies a concrete partition replica on a broker.
type PartitionReplica struct {
	Topic       string        `json:"topic"`
	Partition   int           `json:"partition"`
	BrokerID    int           `json:"brokerID"`
	Role        PartitionRole `json:"role"`
	LeaderEpoch int32         `json:"leaderEpoch"`
}

// PartitionMetadata captures per-partition state that is exposed to clients.
// Leader/Replicas/ISR describe the cluster view for routing, while Replica
// describes the local replica (leader or follower) owned by the responding
// broker.
type PartitionMetadata struct {
	Replica       PartitionReplica `json:"replica"`
	StartOffset   Offset           `json:"startOffset"`
	HighWatermark Offset           `json:"highWatermark"`
	Leader        int              `json:"leader"`
	Replicas      []int            `json:"replicas"`
	ISR           []int            `json:"isr"`
}

// BrokerInfo describes a broker in the cluster.
type BrokerInfo struct {
	BrokerID       int    `json:"brokerID"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Rack           string `json:"rack"`
	HTTPAddr       string `json:"httpAddr,omitempty"`
	ControllerAddr string `json:"controllerAddr,omitempty"`
}

// PartitionAssignment describes replica layout and leader/ISR for a partition.
type PartitionAssignment struct {
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	Replicas    []int  `json:"replicas"`
	ISR         []int  `json:"isr"`
	Leader      int    `json:"leader"`
	LeaderEpoch int32  `json:"leaderEpoch"`
}

// ClusterMetadata captures cluster-wide broker and partition state.
type ClusterMetadata struct {
	ClusterID  string                `json:"clusterID"`
	Version    int64                 `json:"version"`
	Brokers    []BrokerInfo          `json:"brokers"`
	Partitions []PartitionAssignment `json:"partitions"`
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
//
//nolint:revive // Protocol naming is intentionally APIKey across the project.
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
