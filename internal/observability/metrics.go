package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payledger_http_requests_total",
		Help: "HTTP requests by route pattern, method and status code.",
	}, []string{"route", "method", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payledger_http_request_duration_seconds",
		Help:    "HTTP request latency by route pattern.",
		Buckets: []float64{.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"route"})

	GRPCRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payledger_grpc_requests_total",
		Help: "gRPC requests by method and status code.",
	}, []string{"method", "code"})

	GRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payledger_grpc_request_duration_seconds",
		Help:    "gRPC request latency by method.",
		Buckets: []float64{.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"method"})

	TransfersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payledger_transfers_total",
		Help: "Money movements by kind and result.",
	}, []string{"kind", "result"})

	RateLimitDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payledger_ratelimit_decisions_total",
		Help: "Rate limiter decisions: allowed, limited or error (fail-open).",
	}, []string{"decision"})

	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payledger_outbox_published_total",
		Help: "Outbox messages published to Kafka.",
	})

	OutboxErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payledger_outbox_publish_errors_total",
		Help: "Failed outbox batches.",
	})

	OutboxLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "payledger_outbox_lag_seconds",
		Help: "Age of the oldest unpublished outbox message.",
	})

	OutboxBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "payledger_outbox_batch_size",
		Help:    "Messages per published outbox batch.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500},
	})

	ConsumerMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payledger_notifier_messages_total",
		Help: "Consumed messages by result: processed, duplicate, retried, dead_lettered.",
	}, []string{"result"})
)
