package observability

import "github.com/prometheus/client_golang/prometheus"

var Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "source_api_requests_total", Help: "Source requests by route and status."}, []string{"route", "status"})
var Transactions = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "source_database_transaction_seconds", Help: "Source transaction duration."})
var Consumed = prometheus.NewCounter(prometheus.CounterOpts{Name: "cdc_events_consumed_total", Help: "Durably processed records."})
var Rejected = prometheus.NewCounter(prometheus.CounterOpts{Name: "cdc_events_rejected_total", Help: "Records durably quarantined."})
var Retries = prometheus.NewCounter(prometheus.CounterOpts{Name: "cdc_retries_total", Help: "Persistence retries."})
var Processing = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "cdc_processing_seconds", Help: "Partition batch persistence time."})
var AnalyticsWrites = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "analytics_insert_seconds", Help: "Analytics database batch transaction duration."})
var Latency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "cdc_replication_latency_seconds", Help: "Source to analytics delay.", Buckets: []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}})

func init() {
	prometheus.MustRegister(Requests, Transactions, Consumed, Rejected, Retries, Processing, AnalyticsWrites, Latency)
}
