# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Default `REDPANDA_BROKER` changed from a homelab-specific service DNS to `localhost:9092` so the binary is usable on a fresh cluster without overrides
- Default `FINDINGS_TOPIC` changed from `k8s.events.triaged` to `k8s.namespace.findings` to match the documented public contract
- Renamed internal config fields `AITriageEnabled` / `AIMinScore` / `AICooldown` / `AICooldownIncident` to `DownstreamTriageEnabled` / `DownstreamMinScore` / `DownstreamCooldown` / `DownstreamCooldownIncident`. The Finding JSON field `ai_triage_required` is unchanged for backward compatibility.

### Added

- `test-reports/` directory with a structured report (catalogue of every
  test with purpose, expected, and actual results) plus raw evidence
  (`*.txt`, `*.json`, `*.tsv`, `*.out` for both build paths, lint, and
  coverage). Captured against the most recent test run.
- `chart/` directory: a namespace-scoped Helm chart that is validated
  on every PR (lint + render + schema negative tests) and packaged +
  pushed to `oci://ghcr.io/foxj77/charts/autonomous-monitor` on every
  release. See `chart/README.md` for per-knob documentation and
  install examples.
- `helm-publish` job in the release workflow packages the chart and
  pushes it to `oci://ghcr.io/foxj77/charts/autonomous-monitor` on
  every release / versioned manual dispatch.
- `chart` job in the CI workflow runs `helm lint`, three render
  assertions (default values, custom CRB + ingress selector, and four
  schema negative tests) on every PR.

### Fixed

- `.gitignore` no longer permits `coverage.out` to be checked in accidentally; the existing rule is now commented so the next contributor understands why.

## [0.1.0] - 2026-06-01

Initial public release. Source extracted from the homelab repository where it had been running in production across 8 namespaces.
