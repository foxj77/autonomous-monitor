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
  - Services, PVCs, Scaling (HPA)
- **Stateful**: persists findings, restart counts, memory samples in a ConfigMap
- **Suppression**: optional `ConfigMap` keyed by `kind/name/reason` with `*` wildcards
- **AI cooldowns**: throttle expensive downstream AI calls per finding and per severity band
- **Prometheus metrics** on `:8080/metrics`
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

The build requires CGO and `librdkafka-dev` (the binary links against `confluent-kafka-go`). See `Dockerfile` for the reference build environment.

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
            - name: AI_TRIAGE_ENABLED
              value: "true"
            - name: AI_MIN_SCORE
              value: "60"
          ports:
            - name: metrics
              containerPort: 8080
          resources:
            requests: { cpu: 10m, memory: 64Mi }
            limits:   { cpu: 100m, memory: 128Mi }
```

See `manifest/base/` for a complete example with ServiceAccount, Role, RoleBinding, and Service.

### RBAC

The monitor needs **namespace-scoped** read access. Cluster-scoped discovery is only used to enumerate `namespaced` custom resources it can list, not to fetch their data cross-namespace.

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

To monitor **custom resources**, grant `list` on each CRD you care about. The monitor uses `discovery.ServerGroupsAndResources()` to enumerate the namespaced resources it can see, so granting cluster-wide `list` on a CRD group is the cleanest approach:

```yaml
  - apiGroups: ["source.toolkit.fluxcd.io", "helm.toolkit.fluxcd.io"]
    resources: ["*"]
    verbs: [get, list]
```

## Configuration

All configuration is environment-driven.

| Variable | Default | Notes |
|---|---|---|
| `WATCH_NAMESPACE` / `POD_NAMESPACE` | `default` | Target namespace |
| `POLL_INTERVAL` | `60s` | How often to run all checks |
| `METRICS_PORT` | `8080` | Prometheus endpoint port |
| `REDPANDA_BROKER` | `localhost:9092` | Kafka-compatible broker |
| `FINDINGS_TOPIC` | `k8s.namespace.findings` | Topic for JSON findings |
| `STATE_CONFIGMAP_NAME` | `autonomous-monitor-state` | Per-namespace state CM |
| `STATE_WRITE_INTERVAL` | `60s` | Throttle for state CM writes |
| `RESOLVED_FINDING_RETENTION` | `24h` | Drop resolved findings from state after this |
| `AI_TRIAGE_ENABLED` | `true` | Mark findings for AI dispatch |
| `AI_MIN_SCORE` | `60` | Findings at or above this score are flagged |
| `AI_COOLDOWN` | `30m` | Per-finding cooldown between AI dispatches |
| `AI_COOLDOWN_INCIDENT` | `10m` | Shorter cooldown for `incident-likely` findings |
| `SUPPRESS_CONFIGMAP` | _(unset)_ | CM with `kind/name/reason` keys (wildcards `*` allowed) |
| `LOG_SCAN_LINES` | `100` | Lines to tail per container for log scanning |
| `RESOURCE_USAGE_BACKEND` | `metrics-server` | `metrics-server` or `disabled` |
| `MEMORY_WARNING_PERCENT` | `80` | Memory % of limit that triggers a `memory-high` finding |
| `MEMORY_CRITICAL_PERCENT` | `90` | Memory % of limit that triggers a `memory-critical` finding |
| `CPU_WARNING_PERCENT` | `80` | _(reserved for future CPU check)_ |
| `RESTART_WARNING_COUNT` | `3` | Restart-count delta within window that triggers a finding |
| `RESTART_WINDOW` | `10m` | Window for the restart-rate detector |
| `EVENT_LOOKBACK` | `30m` | How far back to consider warning events |
| `CHECK_PODS_ENABLED` | `true` | Toggle individual check families |
| `CHECK_EVENTS_ENABLED` | `true` | |
| `CHECK_LOGS_ENABLED` | `true` | |
| `CHECK_WORKLOADS_ENABLED` | `true` | |
| `CHECK_SCALING_ENABLED` | `true` | |
| `CHECK_RESOURCE_USAGE_ENABLED` | `true` | (set `RESOURCE_USAGE_BACKEND=disabled` to also disable the metrics client) |
| `CHECK_RESOURCE_SPECS_ENABLED` | `true` | |
| `CHECK_CUSTOM_RESOURCES_ENABLED` | `true` | |
| `CHECK_SERVICES_ENABLED` | `true` | |
| `CHECK_PVCS_ENABLED` | `true` | |

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

## Metrics

`autonomous_monitor_checks_total{namespace,check,result}`
`autonomous_monitor_findings_total{namespace,classification,check}`
`autonomous_monitor_active_findings{namespace,classification}`
`autonomous_monitor_ai_dispatch_requests_total{namespace,result}`
`autonomous_monitor_ai_cooldowns_total{namespace}`
`autonomous_monitor_state_writes_total{namespace,result}`
`autonomous_monitor_resource_samples_total{namespace,backend}`
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

This repo uses GitHub Actions to publish:

- A multi-arch container image (`linux/amd64`, `linux/arm64`) to `ghcr.io/foxj77/autonomous-monitor`
- The same image signed with `cosign` (keyless, OIDC)
- A SBOM in SPDX JSON
- A GitHub Release with the raw binaries (linux/amd64, linux/arm64) attached
- The raw binary as an OCI artifact (using `oras`) at `ghcr.io/foxj77/autonomous-monitor:<tag>`

Every push to `main` produces a `:nightly` and `:main` image. Every tag matching `v*.*.*` produces a stable release.

## License

Apache-2.0. See [LICENSE](./LICENSE).

## Security

See [SECURITY.md](./SECURITY.md) for how to report a vulnerability.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).
