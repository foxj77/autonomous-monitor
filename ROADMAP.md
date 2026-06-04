# Roadmap to v1.0.0

The project is currently at **v0.1.0**. The Finding JSON schema, the
`autonomous_monitor_*` Prometheus metric names, and the per-namespace
operational model are all stable. v1.0.0 will be a stability commitment
rather than a feature dump.

This document is the working list. Items below are not promises — they
are the things the maintainer wants to ship before tagging v1.0.0.
Anything not on this list is post-v1.0.0 scope.

## Stability contract for v1.0.0

Once v1.0.0 is tagged, the following will be considered stable and
changed only with a major version bump:

- The `Finding` JSON schema in `finding.go` (field names, types, and the
  `id` derivation).
- The `autonomous_monitor_*` Prometheus metric names.
- The `manifest/base/` kustomize resources and their field-level semantics.
- The environment variable names listed in the README configuration
  table. Deprecated aliases listed in that table may be removed; nothing
  else will be removed without a major bump.

## Pre-1.0.0 work

- [x] **Pure-Go Kafka build path** — `confluent-kafka-go` is the default
      (delivers the richest producer callbacks) but a `kafka_pure` build
      tag now enables `twmb/franz-go` so contributors without a C
      toolchain can `go build` and `go test` on a fresh laptop.
- [x] **Honest naming** — `AI_TRIAGE_ENABLED` and friends are deprecated
      aliases for `DOWNSTREAM_TRIAGE_ENABLED` and friends. The monitor
      never calls a model itself; the new name reflects that. Legacy
      names still work with a one-shot deprecation log.
- [x] **NetworkPolicy in the base manifest** — default-deny ingress with
      an explicit allow for `:8080/metrics`, plus a tight egress allowlist
      (DNS, API server, Kafka broker). Overlays can loosen the selector.
- [x] **`.gitignore` cleanup** — `coverage.out` and friends are no longer
      in the working tree, and the ignore rules are commented so the next
      person does not have to guess.
- [x] **Helm chart** — `chart/` ships a namespace-scoped Helm chart that
      is validated on every PR (lint + render + schema) and packaged
      and pushed to `oci://ghcr.io/foxj77/charts/autonomous-monitor` on
      every release. See [`chart/README.md`](./chart/README.md).
- [x] **End-to-end consumer example** — `examples/quickstart/` (Redpanda
      + monitor + Go consumer in `docker compose`), `examples/consumer/`
      (standalone Go program), and `examples/grafana/` (dashboard JSON).
- [ ] **Integration test against envtest** — current tests are
      `client-go/fake` unit tests. An envtest-driven test that exercises
      the publisher end-to-end against a real `kubelet`/etcd is the
      single largest correctness gap. Tracked in
      [issue TBD](https://github.com/foxj77/autonomous-monitor/issues).
- [x] **Publisher unit test** — the franz-go backend has a hermetic
      round-trip test against an in-process `kfake` broker, gated behind
      `-tags kafka_pure`. The confluent backend is exercised only by
      integration smoke tests today.
- [ ] **Lock the CGO install command in CI** — the CI installs
      `librdkafka-dev` by name; pin the version and verify it matches
      the alpine package version baked into the release image.

## Post-1.0.0 scope (deferred)

- Cluster-wide watching (single monitor, multiple namespaces, with
  leader election). Explicitly **not** in scope for v1.0.0 — the
  per-namespace model is a feature, not a missing capability.
- Built-in AI dispatch (the monitor actually calling a model). This
  remains out of scope. The monitor's job is to emit deterministic
  findings; an AI consumer is someone else's job.
- A web UI. Everything in the monitor is observable via Prometheus
  and the published finding stream; a UI is a separate project.

## Versioning

The project follows [Semantic Versioning](https://semver.org/). Pre-1.0.0
versions (0.x.y) may contain breaking changes to anything not listed in
the stability contract above; the CHANGELOG will call out every break.

## How to influence this list

Open a GitHub issue. Bug reports and small enhancement proposals get
reviewed quickly. Anything that touches the v1.0.0 stability contract
will get a longer discussion.
