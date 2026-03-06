package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

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

var _ = registerMetrics()

func registerMetrics() struct{} {
	prometheus.MustRegister(
		MessagesProduced,
		MessagesConsumed,
		RequestErrors,
		ProduceLatency,
		FetchLatency,
		ReplicationLag,
		ReplicationApplied,
	)

	return struct{}{}
}

// CounterValue returns the current value for a counter series.
// Missing series and write errors are treated as zero.
func CounterValue(counter *prometheus.CounterVec, labels ...string) float64 {
	if counter == nil {
		return 0
	}

	m := &dto.Metric{}
	if err := counter.WithLabelValues(labels...).Write(m); err != nil {
		return 0
	}

	if m.Counter == nil {
		return 0
	}

	return m.Counter.GetValue()
}

// EnsureCounterAtLeast bumps a counter series to at least value.
func EnsureCounterAtLeast(counter *prometheus.CounterVec, value float64, labels ...string) {
	if counter == nil || value <= 0 {
		return
	}

	current := CounterValue(counter, labels...)
	if value <= current {
		return
	}

	counter.WithLabelValues(labels...).Add(value - current)
}
