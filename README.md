# autonomous-monitor

A small, generic Kubernetes namespace monitor that runs **continuous deterministic checks** in a target namespace, tracks state across polls, and publishes JSON findings to a Kafka-compatible broker.

It is **not** an event-replacement and it is **not** an AI agent. It does the boring, cheap, repeatable work — pending pods, OOM kills, deployment generation lag, stuck rollouts, memory trending toward limits, generic custom-resource conditions — and hands suspicious findings to whatever consumer you want (a Kafka topic, a SIEM, a simple curl). The monitor never writes to the cluster, never calls a model, and never patches anything.

## How it works

```
       [autonomous-monitor] ──── in-cluster, per-namespace
                │
                ├── reads Kubernetes API (pods, events, workloads, custom resources, logs, metrics)
                ├── reads / writes state ConfigMap (findings, restart observations, memory samples)
                └── publishes findings ──▶ Kafka-compatible broker (default topic: k8s.namespace.findings)
                                                          │
                                                          ▼
                                                  [your consumer / agent / pager]
```

One monitor per namespace. No cluster-wide permissions. No port forwarding. No external state store.

## Features

- **Ten check families** (all individually toggleable):
  - Pods (waiting reasons, OOMKilled, restart rate, not-ready)
  - Events (warning events within a configurable lookback)
  - Workloads (Deployments / StatefulSets / DaemonSets — replica shortfall, generation lag)
  - Resource specs (missing memory / CPU requests and limits)
  - Resource usage (memory % of limit, trend detection — `metrics-server` backend)
  - Custom resources (dynamic-client discovery, `observedGeneration` lag, condition health)
  - Logs (tail last N lines, scan for panics, fatal, OOMKilled, TLS errors, repeated errors)
  - Services (selectors matching no pods, pending LoadBalancers)
  - PVCs (Pending / Lost claims, true PVC conditions)
  - Scaling (HPA unhealthy conditions, maxed-out replicas, desired replica lag)
- **Stateful**: persists findings, restart counts, memory samples in a ConfigMap
- **Suppression**: optional `ConfigMap` keyed by `kind/name/reason` with `*` wildcards
- **AI cooldowns**: throttle expensive downstream AI calls per finding and per severity band
- **Prometheus metrics** on `:8080/metrics`
- **Health endpoints**: `/healthz` (liveness — 503 when the poll loop is wedged) and `/readyz` (readiness — 503 until the first poll completes)
- **Generic CRD discovery** — no hardcoded group/version lists

## Install

### Container image

```bash
docker pull ghcr.io/foxj77/autonomous-monitor:latest
```

The image is multi-arch (`linux/amd64`, `linux/arm64`).

### From source

```bash
go install github.com/foxj77/autonomous-monitor@latest
```

The build has two variants:

| Variant | Build command | When to use |
|---|---|---|
| **Default** (production) | `CGO_ENABLED=1 go build -tags musl -o autonomous-monitor .` | Released images. Uses `confluent-kafka-go/v2` with `librdkafka` for the richest producer callbacks. |
| **Pure Go** (developer) | `go build -tags 'musl kafka_pure' -o autonomous-monitor .` | No CGO, no `librdkafka`. Uses `twmb/franz-go` for the same delivery-confirmation contract. Use this if you don't have a C toolchain. |

The default build requires `librdkafka-dev` (the binary links against `confluent-kafka-go`). See `Dockerfile` for the reference build environment. See `Dockerfile.pure` for the no-CGO build.

### Try it locally (no cluster required)

The fastest way to see the monitor in action is the [examples/quickstart](./examples/quickstart)
`docker compose` stack, which runs Redpanda, the monitor, and a Go consumer
on a single host:

```bash
cd examples/quickstart
docker compose up --build
docker compose logs -f consumer
```

## Helm chart

The monitor ships with a Helm chart at [`chart/`](./chart) and
publishes it to GHCR as an OCI artifact on every release. Install
the latest published version with:

```bash
helm install autonomous-monitor \
  oci://ghcr.io/foxj77/charts/autonomous-monitor \
  --version 0.1.0 \
  --namespace my-namespace \
  --create-namespace \
  --set kafka.broker=my-broker.my-namespace.svc.cluster.local:9092 \
  --set kafka.topic=k8s.namespace.findings
```

The chart is namespace-scoped (one release per target namespace) and
includes a default-deny `NetworkPolicy` plus the `ServiceAccount`,
`Role`, and `RoleBinding` the monitor needs. See
[`chart/values.yaml`](./chart/values.yaml) for the full set of knobs
and [`chart/README.md`](./chart/README.md) for a per-knob explanation.

The chart is validated on every PR (`helm lint` + render asserts +
schema negative tests) and packaged + pushed to `oci://ghcr.io/foxj77/charts`
on every release.

## Quick start

The monitor reads its namespace from the `WATCH_NAMESPACE` or `POD_NAMESPACE` env var, and runs continuous polls with a default interval of 60s.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: autonomous-monitor
  namespace: my-app
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: autonomous-monitor }
  template:
    metadata:
      labels: { app.kubernetes.io/name: autonomous-monitor }
    spec:
      serviceAccountName: autonomous-monitor
      containers:
        - name: autonomous-monitor
          image: ghcr.io/foxj77/autonomous-monitor:latest
          env:
            - name: REDPANDA_BROKER
              value: redpanda.redpanda.svc.cluster.local:9093
            - name: FINDINGS_TOPIC
              value: k8s.namespace.findings
            - name: STATE_CONFIGMAP_NAME
              value: autonomous-monitor-state
            - name: DOWNSTREAM_TRIAGE_ENABLED
              value: "true"
            - name: DOWNSTREAM_MIN_SCORE
              value: "60"
          ports:
            - name: metrics
              containerPort: 8080
          resources:
            requests: { cpu: 10m, memory: 64Mi }
            limits:   { cpu: 100m, memory: 128Mi }
```

See `manifest/base/` for a complete namespace-scoped example with ServiceAccount, Role, RoleBinding, and Service.

### RBAC

The monitor needs **namespace-scoped** read access. The default manifests do not grant cluster-wide resource reads.

Minimum:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: autonomous-monitor }
rules:
  - apiGroups: [""]
    resources: [pods, pods/log, events, configmaps, services, persistentvolumeclaims]
    verbs: [get, list, watch]
  - apiGroups: ["apps"]
    resources: [deployments, statefulsets, daemonsets, replicasets]
    verbs: [get, list, watch]
  - apiGroups: ["batch"]
    resources: [jobs, cronjobs]
    verbs: [get, list, watch]
  - apiGroups: ["autoscaling"]
    resources: [horizontalpodautoscalers]
    verbs: [get, list, watch]
  # ConfigMap writes for state persistence
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [create, update, patch]
```

To monitor **custom resources**, grant namespaced access to each custom resource type you care about. The monitor uses `discovery.ServerGroupsAndResources()` to enumerate namespaced resources, then lists only in `WATCH_NAMESPACE`:

```yaml
  - apiGroups: ["source.toolkit.fluxcd.io", "helm.toolkit.fluxcd.io"]
    resources: ["*"]
    verbs: [get, list, watch]
```

Add those rules to the monitor's namespace `Role` for each namespace you monitor. Most clusters allow authenticated API discovery without additional RBAC; if your cluster restricts discovery endpoints, use `manifest/overlays/cluster-discovery` to grant the narrow cluster-scoped discovery permission. That overlay does not grant cross-namespace resource access.

## Configuration

All configuration is environment-driven.

| Variable | Default | Notes |
|---|---|---|
| `WATCH_NAMESPACE` / `POD_NAMESPACE` | `default` | Target namespace |
| `POLL_INTERVAL` | `60s` | How often to run all checks |
| `METRICS_PORT` | `8080` | Prometheus endpoint port |
| `REDPANDA_BROKER` | `localhost:9092` | Kafka-compatible broker |
| `FINDINGS_TOPIC` | `k8s.namespace.findings` | Topic for JSON findings |
| `PUBLISH_TIMEOUT` | `10s` | How long to wait for broker delivery confirmation before retrying later |
| `CHECK_TIMEOUT` | `30s` | Maximum time a single check family can spend in one poll |
| `STATE_CONFIGMAP_NAME` | `autonomous-monitor-state` | Per-namespace state CM |
| `STATE_WRITE_INTERVAL` | `60s` | Throttle for state CM writes |
| `RESOLVED_FINDING_RETENTION` | `24h` | Drop resolved findings from state after this |
| `OBSERVATION_RETENTION` | `24h` | Drop stale restart/memory observations after this |
| `MAX_OBSERVATIONS` | `5000` | Hard cap for observation entries kept in state |
| `MAX_FINDINGS` | `2000` | Hard cap for finding entries kept in state |
| `MAX_STATE_BYTES` | `921600` | Maximum serialized state size before a write is refused |
| `DOWNSTREAM_TRIAGE_ENABLED` | `true` | Mark findings for downstream triage. The monitor never calls a model itself; it sets `ai_triage_required: true` on the published finding and expects a consumer to act on it. |
| `DOWNSTREAM_MIN_SCORE` | `60` | Findings at or above this score are flagged for downstream triage |
| `DOWNSTREAM_COOLDOWN` | `30m` | Per-finding cooldown between triage flag emissions |
| `DOWNSTREAM_COOLDOWN_INCIDENT` | `10m` | Shorter cooldown for `incident-likely` findings |
| `AI_TRIAGE_ENABLED` | _(deprecated)_ | Alias for `DOWNSTREAM_TRIAGE_ENABLED`. Will be removed in v1.0.0. |
| `AI_MIN_SCORE` | _(deprecated)_ | Alias for `DOWNSTREAM_MIN_SCORE`. |
| `AI_COOLDOWN` | _(deprecated)_ | Alias for `DOWNSTREAM_COOLDOWN`. |
| `AI_COOLDOWN_INCIDENT` | _(deprecated)_ | Alias for `DOWNSTREAM_COOLDOWN_INCIDENT`. |
| `SUPPRESS_CONFIGMAP` | _(unset)_ | CM with `kind/name/reason` keys (wildcards `*` allowed) |
| `LOG_SCAN_LINES` | `100` | Lines to tail per container for log scanning |
| `RESOURCE_USAGE_BACKEND` | `metrics-server` | `metrics-server` or `disabled` |
| `MEMORY_WARNING_PERCENT` | `80` | Memory % of limit that triggers a `memory-high` finding |
| `MEMORY_CRITICAL_PERCENT` | `90` | Memory % of limit that triggers a `memory-critical` finding |
| `RESTART_WARNING_COUNT` | `3` | Restart-count delta within window that triggers a finding |
| `RESTART_WINDOW` | `10m` | Window for the restart-rate detector |
| `EVENT_LOOKBACK` | `30m` | How far back to consider warning events |
| `CUSTOM_RESOURCE_ALLOWLIST` | _(unset)_ | Comma-separated custom resource groups or `group/resource` patterns to scan |
| `CUSTOM_RESOURCE_EXCLUDELIST` | _(unset)_ | Comma-separated custom resource groups or `group/resource` patterns to skip |
| `CUSTOM_RESOURCE_GROUPS` | _(unset)_ | Alias for `CUSTOM_RESOURCE_ALLOWLIST` |
| `CUSTOM_RESOURCE_EXCLUDE_GROUPS` | _(unset)_ | Alias for `CUSTOM_RESOURCE_EXCLUDELIST` |
| `CUSTOM_RESOURCE_DISCOVERY_TTL` | `10m` | How long to cache API discovery results for custom resource scanning |
| `HEALTH_MAX_POLL_GAP` | _(formula)_ | Maximum duration between completed polls before `/healthz` returns 503. Defaults to `max(3 * POLL_INTERVAL, POLL_INTERVAL + CHECK_TIMEOUT)`. |
| `CHECK_PODS_ENABLED` | `true` | Toggle individual check families |
| `CHECK_EVENTS_ENABLED` | `true` | |
| `CHECK_LOGS_ENABLED` | `true` | |
| `CHECK_WORKLOADS_ENABLED` | `true` | |
| `CHECK_SCALING_ENABLED` | `true` | HPA condition and replica pressure checks |
| `CHECK_RESOURCE_USAGE_ENABLED` | `true` | (set `RESOURCE_USAGE_BACKEND=disabled` to also disable the metrics client) |
| `CHECK_RESOURCE_SPECS_ENABLED` | `true` | |
| `CHECK_CUSTOM_RESOURCES_ENABLED` | `true` | |
| `CHECK_SERVICES_ENABLED` | `true` | Service selector and LoadBalancer checks |
| `CHECK_PVCS_ENABLED` | `true` | PVC phase and condition checks |

## Output contract

The monitor publishes a JSON `Finding` for every state change (new finding, score change, classification change, resolved). A finding looks like:

```json
{
  "source": "autonomous-monitor",
  "payload_type": "namespace_finding",
  "id": "<sha256(namespace|kind|name|check|reason)>",
  "namespace": "cert-manager",
  "kind": "Pod",
  "name": "cert-manager-webhook-0",
  "severity": "high",
  "classification": "degraded",
  "score": 65,
  "check": "pod-health",
  "reason": "not-ready",
  "status": "ongoing",
  "first_seen": "2026-05-06T12:00:00Z",
  "last_seen": "2026-05-06T12:00:30Z",
  "evidence": ["pod has not been Ready for 5m"],
  "prediction": "",
  "matching_kubernetes_event_found": false,
  "ai_triage_required": true,
  "cooldown_until": "2026-05-06T12:30:30Z"
}
```

The `id` is deterministic. A consumer can use it to deduplicate across re-emits, restarts, and re-deploys.

Resolution events (status: `resolved`) are emitted when a previously-ongoing finding no longer appears in a poll cycle.

## Health endpoints

The monitor exposes two health endpoints on `METRICS_PORT` (default `8080`):

| Endpoint | Purpose | Behavior |
|---|---|---|
| `/healthz` | Liveness | Returns `200 ok` while the poll loop is completing polls within the allowed gap. Returns `503` if no poll has completed within `HEALTH_MAX_POLL_GAP` (default: `max(3 * POLL_INTERVAL, POLL_INTERVAL + CHECK_TIMEOUT)`). Before the first poll completes the process is still starting up and `/healthz` returns `200` — coordinate with `livenessProbe.initialDelaySeconds`. |
| `/readyz` | Readiness | Returns `200 ok` once the first poll has completed (state loaded, first scan done). Returns `503` before that, preventing traffic from reaching the pod during initial startup. |

The Kubernetes probes in the chart and manifest point at these endpoints. The `livenessProbe.initialDelaySeconds` (default `30s`) must be large enough to cover at least one full poll cycle before Kubernetes starts checking liveness.

## Metrics

`autonomous_monitor_checks_total{namespace,check,result}`
`autonomous_monitor_findings_total{namespace,classification,check}`
`autonomous_monitor_active_findings{namespace,classification}`
`autonomous_monitor_ai_dispatch_requests_total{namespace,result}`
`autonomous_monitor_ai_cooldowns_total{namespace}`
`autonomous_monitor_state_writes_total{namespace,result}`
`autonomous_monitor_state_bytes{namespace}`
`autonomous_monitor_state_findings{namespace}`
`autonomous_monitor_state_observations{namespace}`
`autonomous_monitor_state_prunes_total{namespace,type,reason}`
`autonomous_monitor_publish_attempts_total{namespace,result}`
`autonomous_monitor_resource_samples_total{namespace,backend}`
`autonomous_monitor_custom_resource_scans_total{namespace,result}`
`autonomous_monitor_poll_duration_seconds{namespace}`

All metrics are exposed on `METRICS_PORT/metrics`.

## Development

```bash
# run tests
go test -race -count=1 ./...

# run linter
golangci-lint run

# build the binary
CGO_ENABLED=1 go build -tags musl -o autonomous-monitor .
```

A reference build environment is in the `Dockerfile` (`golang:1.23-alpine` with `librdkafka-dev`).

## Releasing

This repo uses release-please and GitHub Actions to publish:

- A release PR that bumps `version.txt` and `CHANGELOG.md`
- A GitHub Release and `v*.*.*` tag when the release PR is merged
- A publish workflow triggered by that GitHub Release, with a `Release Please`
  workflow fallback for default-token releases where GitHub suppresses the
  downstream `release` event

- A multi-arch container image (`linux/amd64`, `linux/arm64`) to `ghcr.io/foxj77/autonomous-monitor`
- The same image signed with `cosign` (keyless, OIDC)
- A SBOM in SPDX JSON
- A GitHub Release with the raw binaries (linux/amd64, linux/arm64) attached
- The raw binaries as an OCI artifact (using `oras`) at `ghcr.io/foxj77/autonomous-monitor:<tag>-binary`

Stable releases publish image tags without the leading `v`: `1.2.3`, `1.2`, `1` for non-`v0` releases, `latest`, and `sha-<short>`. Manual workflow dispatch without a version publishes `edge` and `sha-<short>` only.

For the fully automatic path, the repository Actions setting must allow
workflows to create pull requests. A `RELEASE_PLEASE_TOKEN` secret with `repo`
and `workflow` scopes is still recommended, matching the MCP memory server
setup, because it lets the GitHub Release trigger downstream workflows directly.

## License

Apache-2.0. See [LICENSE](./LICENSE).

## Security

See [SECURITY.md](./SECURITY.md) for how to report a vulnerability.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## Roadmap

See [ROADMAP.md](./ROADMAP.md) for the path to v1.0.0 and what is in
scope versus explicitly out of scope.

## Examples

See [examples/](./examples) for a runnable quickstart (Redpanda +
monitor + consumer in `docker compose`), a standalone Go consumer you
can fork, and a Grafana dashboard JSON.
