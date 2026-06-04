package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	checksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_checks_total",
		Help: "Checks run by family and result.",
	}, []string{"namespace", "check", "result"})
	findingsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_findings_total",
		Help: "Findings created by classification and check.",
	}, []string{"namespace", "classification", "check"})
	activeFindings = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "autonomous_monitor_active_findings",
		Help: "Current active finding count by classification.",
	}, []string{"namespace", "classification"})
	aiDispatchRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_ai_dispatch_requests_total",
		Help: "Findings marked for AI dispatch.",
	}, []string{"namespace", "result"})
	aiCooldowns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_ai_cooldowns_total",
		Help: "AI dispatch requests suppressed by cooldown.",
	}, []string{"namespace"})
	stateWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_state_writes_total",
		Help: "ConfigMap state writes by result.",
	}, []string{"namespace", "result"})
	stateBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "autonomous_monitor_state_bytes",
		Help: "Serialized monitor state size in bytes.",
	}, []string{"namespace"})
	stateFindings = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "autonomous_monitor_state_findings",
		Help: "Findings currently held in monitor state.",
	}, []string{"namespace"})
	stateObservations = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "autonomous_monitor_state_observations",
		Help: "Observations currently held in monitor state.",
	}, []string{"namespace"})
	statePrunes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_state_prunes_total",
		Help: "State entries pruned by type and reason.",
	}, []string{"namespace", "type", "reason"})
	publishAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_publish_attempts_total",
		Help: "Finding publish attempts by result.",
	}, []string{"namespace", "result"})
	resourceSamples = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_resource_samples_total",
		Help: "Resource samples collected by backend.",
	}, []string{"namespace", "backend"})
	customResourceScans = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "autonomous_monitor_custom_resource_scans_total",
		Help: "Custom resources scanned, skipped, or errored.",
	}, []string{"namespace", "result"})
	pollDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "autonomous_monitor_poll_duration_seconds",
		Help:    "Poll duration in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	}, []string{"namespace"})
)

func init() {
	prometheus.MustRegister(
		checksTotal,
		findingsTotal,
		activeFindings,
		aiDispatchRequests,
		aiCooldowns,
		stateWrites,
		stateBytes,
		stateFindings,
		stateObservations,
		statePrunes,
		publishAttempts,
		resourceSamples,
		customResourceScans,
		pollDuration,
	)
}
