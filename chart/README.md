# `autonomous-monitor` Helm chart

A Helm chart for deploying `autonomous-monitor` to a Kubernetes cluster.
The chart is namespace-scoped: one release per target namespace, with
all the manifests constrained to that namespace.

The chart is published to GHCR as an OCI artifact. Install the latest
release with:

```bash
helm install autonomous-monitor \
  oci://ghcr.io/foxj77/charts/autonomous-monitor \
  --version 0.1.0 \
  --namespace my-namespace \
  --create-namespace
```

## What this chart installs

For a single release:

- `Deployment` (1 replica) — runs the monitor with the image, env vars,
  resources, probes, and security context described in
  [`values.yaml`](./values.yaml). The liveness probe hits `/healthz` (poll-loop
  liveness) and the readiness probe hits `/readyz` (first-poll gate).
- `Service` (ClusterIP) — exposes the Prometheus metrics port
  (`8080`).
- `ServiceAccount` — namespaced; can be opted out via
  `serviceAccount.create: false`.
- `Role` + `RoleBinding` — namespace-scoped reads for the resources the
  monitor watches, plus ConfigMap writes for the state ConfigMap. The
  Role is extended at install time with any `customResources.rbacRules`
  you supply.
- `NetworkPolicy` — default-deny ingress, with an explicit allow for
  the metrics port. Egress is allowlisted to DNS, the API server, and
  the Kafka broker on 9092/9093. Disable with `networkPolicy.enabled: false`
  for clusters without a NetworkPolicy controller.

## Configuration

Every value in [`values.yaml`](./values.yaml) is documented inline. The
schema in [`values.schema.json`](./values.schema.json) rejects invalid
values at install time (e.g. `image.pullPolicy: Sometimes`,
`service.type: EvilService`, `thresholds.memoryCriticalPercent: 200`).

The most commonly customised values:

| Value | Purpose |
|---|---|
| `kafka.broker` | The Kafka-compatible broker the monitor publishes to. |
| `kafka.topic` | The topic the monitor writes findings to. |
| `watch.namespace` | Defaults to the release namespace; override to watch a different one. |
| `checks.*` | Toggle individual check families. |
| `thresholds.*` | Poll interval, retention, memory/restart thresholds. |
| `downstreamTriage.*` | Score threshold and cooldown for the `ai_triage_required` flag. |
| `suppression.configMapName` | Optional ConfigMap of `kind/name/reason` keys to suppress. |
| `customResources.allowlist` / `excludelist` | Filter the API groups scanned via the dynamic client. |
| `customResources.rbacRules` | Extend the Role with namespaced reads for specific CRDs. |
| `networkPolicy.ingress.metrics.from` | Restrict the metrics port to a `podSelector`/`namespaceSelector`. |
| `health.maxPollGap` | Override the default liveness gap formula (`max(3*pollInterval, pollInterval+checkTimeout)`). Example: `"5m"`. |

## Examples

### Minimal: monitor a single namespace

```bash
helm install autonomous-monitor \
  oci://ghcr.io/foxj77/charts/autonomous-monitor \
  --version 0.1.0 \
  --namespace my-app --create-namespace \
  --set kafka.broker=redpanda.redpanda.svc.cluster.local:9092
```

### Monitor a namespace with Flux custom resources

```bash
helm install autonomous-monitor \
  oci://ghcr.io/foxj77/charts/autonomous-monitor \
  --version 0.1.0 \
  --namespace flux-system --create-namespace \
  --set kafka.broker=broker.kafka.svc.cluster.local:9092 \
  --set customResources.rbacRules[0].apiGroups="{source.toolkit.fluxcd.io,helm.toolkit.fluxcd.io,kustomize.toolkit.fluxcd.io,notification.toolkit.fluxcd.io}" \
  --set customResources.rbacRules[0].resources="{\*}" \
  --set customResources.rbacRules[0].verbs="{get,list,watch}"
```

### Lock metrics scraping to a Prometheus namespace

```bash
helm install autonomous-monitor \
  oci://ghcr.io/foxj77/charts/autonomous-monitor \
  --version 0.1.0 \
  --namespace my-app --create-namespace \
  --set kafka.broker=broker.kafka.svc.cluster.local:9092 \
  --set networkPolicy.ingress.metrics.from.namespaceSelector.matchLabels.kubernetes.io/metadata.name=monitoring
```

### Pin a specific image tag (recommended for production)

```bash
helm install autonomous-monitor \
  oci://ghcr.io/foxj77/charts/autonomous-monitor \
  --version 0.1.0 \
  --namespace my-app --create-namespace \
  --set image.tag=0.1.0
```

## CI

Every PR runs the chart through `helm lint` plus three render
assertions (default values, custom values with CRB rules + a restricted
metrics-ingress selector, and four negative schema tests). See
[`.github/workflows/ci.yaml`](../.github/workflows/ci.yaml).

Every release packages the chart and pushes it to
`oci://ghcr.io/foxj77/charts/autonomous-monitor`. See
[`.github/workflows/release.yaml`](../.github/workflows/release.yaml).
