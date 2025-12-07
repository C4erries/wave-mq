package observability

import "github.com/prometheus/client_golang/prometheus"

var (
	MessagesProduced = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "wavemq",
			Name:      "messages_produced_total",
			Help:      "Total number of messages produced.",
		},
		[]string{"topic", "partition"},
	)
	MessagesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "wavemq",
			Name:      "messages_consumed_total",
			Help:      "Total number of messages consumed.",
		},
		[]string{"topic", "partition"},
	)
	RequestErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "wavemq",
			Name:      "request_errors_total",
			Help:      "Total number of request errors by component and operation.",
		},
		[]string{"component", "operation"},
	)
	ProduceLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "wavemq",
			Name:      "produce_latency_seconds",
			Help:      "Produce latency in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
		},
		[]string{"topic", "partition"},
	)
	FetchLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "wavemq",
			Name:      "fetch_latency_seconds",
			Help:      "Fetch latency in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
		},
		[]string{"topic", "partition"},
	)
	ReplicationLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "wavemq",
			Name:      "replication_lag_offsets",
			Help:      "Follower replication lag in offsets (leader high watermark - last applied).",
		},
		[]string{"topic", "partition", "broker"},
	)
	ReplicationApplied = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "wavemq",
			Name:      "replication_applied_total",
			Help:      "Total number of records applied on follower replication.",
		},
		[]string{"topic", "partition", "broker"},
	)
)

func init() {
	prometheus.MustRegister(
		MessagesProduced,
		MessagesConsumed,
		RequestErrors,
		ProduceLatency,
		FetchLatency,
		ReplicationLag,
		ReplicationApplied,
	)
}
