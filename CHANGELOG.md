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

## [0.1.0] - 2026-06-01

Initial public release. Source extracted from the homelab repository where it had been running in production across 8 namespaces.
