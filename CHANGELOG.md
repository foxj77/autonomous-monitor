# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0](https://github.com/foxj77/autonomous-monitor/compare/v0.2.1...v0.3.0) (2026-06-07)


### Features

* add /healthz and /readyz endpoints reflecting poll liveness ([6d42e2a](https://github.com/foxj77/autonomous-monitor/commit/6d42e2ab5a1ffc6173baf593b8f34dd8bb27fe00))


### Bug Fixes

* **ci:** run go test with CGO_ENABLED=1 so -race works ([38f47b0](https://github.com/foxj77/autonomous-monitor/commit/38f47b07cb6744d94d37604156e412dbc5b47be6))
* eliminate poll-timeout data race and list shared resources once per poll ([7dd5c56](https://github.com/foxj77/autonomous-monitor/commit/7dd5c56b82e7821b6f1d0fa5323c17940e902784))


### Refactors

* drop CGO confluent-kafka-go path, franz-go is the only publisher ([f07c292](https://github.com/foxj77/autonomous-monitor/commit/f07c292b6e2d5def211877a466c1fa3a0e33435c))

## [0.2.1](https://github.com/foxj77/autonomous-monitor/compare/v0.2.0...v0.2.1) (2026-06-05)


### Bug Fixes

* publish releases after release-please fallback ([a085a57](https://github.com/foxj77/autonomous-monitor/commit/a085a57f594d18b2f6555005b2d8e13aadcf9240))
* sign semver container tag ([69b0067](https://github.com/foxj77/autonomous-monitor/commit/69b006760dde5f228ab25a3a47cb562c6827141a))

## [0.2.0](https://github.com/foxj77/autonomous-monitor/compare/v0.1.0...v0.2.0) (2026-06-04)


### Features

* add pure-Go Kafka path, helm chart, examples, and test reports ([0b31e1c](https://github.com/foxj77/autonomous-monitor/commit/0b31e1c7def595d08f5dfbd8539fe2b713673fc5))
* bound per-check poll latency ([5674f7d](https://github.com/foxj77/autonomous-monitor/commit/5674f7d3bd32d05cc372cd68c24c1350b7ab3cbd))
* control custom resource scanning ([763e93b](https://github.com/foxj77/autonomous-monitor/commit/763e93beba33bd8e7ccce0f21bb870ae94587191))
* harden state and align releases ([b7b196d](https://github.com/foxj77/autonomous-monitor/commit/b7b196d4abf2b4107012dbed07479e6968d0cdef))
* initial public release of autonomous-monitor ([4b7f7b8](https://github.com/foxj77/autonomous-monitor/commit/4b7f7b87847cad6d964aeef0f1240faf82dffa99))


### Bug Fixes

* **ci:** align Go and lint toolchain ([#13](https://github.com/foxj77/autonomous-monitor/issues/13)) ([075eb1e](https://github.com/foxj77/autonomous-monitor/commit/075eb1e8a7c7e7c883203d76ab9c831a0fa049f2))
* **ci:** bump golangci-lint to v2 to match .golangci.yml v2 schema ([b4ad233](https://github.com/foxj77/autonomous-monitor/commit/b4ad23385fedd3c27e1ae673d6fed8c44e7179e7))
* **ci:** bump golangci-lint-action to v7 for golangci-lint v2 support ([78469ba](https://github.com/foxj77/autonomous-monitor/commit/78469ba181aa9791a6da45e107dac734ae3929e5))
* **ci:** drop arm64 cross-compile from CI build job ([b8391ff](https://github.com/foxj77/autonomous-monitor/commit/b8391ff2203d425b0a367cdb5b17d46d96c81226))
* **release:** checkout repo in OCI artifact job so README.md is available ([b2df863](https://github.com/foxj77/autonomous-monitor/commit/b2df8635085bac895afc8a570fa3f6b669f5a1f4))
* **release:** install oras via curl instead of oras-project/setup-oras ([656e8e4](https://github.com/foxj77/autonomous-monitor/commit/656e8e4d5527aae62aea01c5c3d575bec05ec3e8))
* **release:** pin oras to a real release version v1.3.2 ([765dd90](https://github.com/foxj77/autonomous-monitor/commit/765dd90cb5eb93a97b08da064b6136b07e6b73bb))
* **release:** use buildx imagetools Digest header for cosign signing ([256ff83](https://github.com/foxj77/autonomous-monitor/commit/256ff83e737576786b3e5ca38412af037bdd2319))
* **release:** use Docker buildx for multi-arch builds and OCI signing ([5ceaea6](https://github.com/foxj77/autonomous-monitor/commit/5ceaea6f61f0829d3306adc84ec0fc7c07e550d4))
* **release:** use latest oras CLI in setup-oras action ([40e34d4](https://github.com/foxj77/autonomous-monitor/commit/40e34d445eba2e7871a62cbd008a0cf0da963e24))
* satisfy custom resource lint ([98753eb](https://github.com/foxj77/autonomous-monitor/commit/98753eb5366bc0fcac00050d305c81329cb1643d))
* **scorecard:** pin resolvable action version ([#14](https://github.com/foxj77/autonomous-monitor/issues/14)) ([2ebde1b](https://github.com/foxj77/autonomous-monitor/commit/2ebde1bdbec96ab85800f215e26d66b05bdd55a2))
* **scorecard:** remove global write permissions ([#15](https://github.com/foxj77/autonomous-monitor/issues/15)) ([09af9b9](https://github.com/foxj77/autonomous-monitor/commit/09af9b9e5f0c3737ef27fcf17945d26bfb9984d5))


### Performance

* **checks:** pre-allocate findings slices where size is known ([559bda2](https://github.com/foxj77/autonomous-monitor/commit/559bda208db5aa4cfeeab2156856428fd3ee0412))

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
