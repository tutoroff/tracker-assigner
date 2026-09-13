package http

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// MetricTicketsAssigned counts total tickets assigned by group and assignee.
	MetricTicketsAssigned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tracker_assigner",
			Name:      "tickets_assigned_total",
			Help:      "Total number of tickets assigned to team members",
		},
		[]string{"group", "assignee"},
	)

	// MetricPendingQueueSize reports the current count of tickets in pending queue.
	MetricPendingQueueSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tracker_assigner",
			Name:      "pending_queue_size",
			Help:      "Current number of tickets waiting in Pending Queue",
		},
	)

	// MetricDLQSize reports the current count of failed events in Dead Letter Queue.
	MetricDLQSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tracker_assigner",
			Name:      "dlq_size",
			Help:      "Current number of events waiting in Dead Letter Queue",
		},
	)

	// MetricWebhookRequests counts incoming webhooks by status and event type.
	MetricWebhookRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tracker_assigner",
			Name:      "webhook_requests_total",
			Help:      "Total number of incoming webhook requests",
		},
		[]string{"status", "event"},
	)

	// MetricWebhookDuration tracks webhook processing latency.
	MetricWebhookDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tracker_assigner",
			Name:      "webhook_duration_seconds",
			Help:      "Duration of webhook processing in seconds",
			Buckets:   prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(MetricTicketsAssigned)
	prometheus.MustRegister(MetricPendingQueueSize)
	prometheus.MustRegister(MetricDLQSize)
	prometheus.MustRegister(MetricWebhookRequests)
	prometheus.MustRegister(MetricWebhookDuration)
}

// MetricsHandler returns the HTTP handler for Prometheus scraping.
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
