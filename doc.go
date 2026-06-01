// Package main implements autonomous-monitor, a small generic Kubernetes
// namespace monitor that runs continuous deterministic checks and publishes
// JSON findings to a Kafka-compatible broker.
//
// One monitor runs per target namespace. It reads the Kubernetes API
// (pods, events, workloads, custom resources, logs, metrics), tracks state
// across polls in a ConfigMap, and emits Findings whenever a check crosses
// a configured threshold. It never writes to the cluster except for its own
// state ConfigMap, never calls an AI model directly, and never patches
// arbitrary resources.
//
// Configuration is environment-driven. See the README for the full list of
// environment variables. The published Finding JSON schema is treated as a
// stable public contract — see finding.Finding.
package main
