# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Default `REDPANDA_BROKER` changed from a homelab-specific service DNS to `localhost:9092` so the binary is usable on a fresh cluster without overrides
- Default `FINDINGS_TOPIC` changed from `k8s.events.triaged` to `k8s.namespace.findings` to match the documented public contract

### Added

- Comprehensive unit tests for previously untested check logic (`checks_test.go`)
- Delivery-aware publishing: Kafka sends now wait for broker delivery confirmation, failed publishes are retried, and publish attempts are exposed via metrics
- `PUBLISH_TIMEOUT` configuration for bounding Kafka delivery confirmation waits
- Release workflow now publishes raw binaries as a separate `<tag>-binary` OCI artifact, keeping container image tags unambiguous
- Service, PVC, and HPA scaling checks are now implemented and wired into the monitor poll loop
- Poll-level tests now verify configured check families actually run when enabled

## [0.1.0] - 2026-06-01

Initial public release. Source extracted from the homelab repository where it had been running in production across 8 namespaces.
