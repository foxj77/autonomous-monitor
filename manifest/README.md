# manifest/

This directory contains a kustomize **base** for deploying `autonomous-monitor` to a single target namespace.

## What this is

- A minimal, generic set of Kubernetes resources (Namespace, ServiceAccount, Role, RoleBinding, NetworkPolicy, Deployment, Service).
- Safe defaults: non-root, read-only root filesystem, dropped capabilities, `seccompProfile: RuntimeDefault`, liveness/readiness probes.
- The Role grants the minimum namespace-scoped reads required for built-in resources and state ConfigMap writes.
- The NetworkPolicy locks down ingress to the metrics port and egress to DNS, the API server, and the Kafka broker. Tighten the `from:` selector in your overlay if you do not want cluster-wide scrape access.
- The Deployment's `WATCH_NAMESPACE` is wired from the pod's own namespace, so you can copy this base into per-namespace overlays and it Just Works.

## How to use it

### Option A — copy and overlay (recommended for GitOps)

In your cluster repo:

```
clusters/
  my-cluster/
    apps/
      autonomous-monitor/
        cert-manager/
          kustomization.yaml   # includes ../../base + namespace patch
        flux-system/
          kustomization.yaml
        ...
```

Each overlay sets the namespace and any env-var overrides.

### Option B — single Deployment watching the cluster

If you only want to monitor one namespace from one Deployment, the simpler path is the example in the top-level `README.md`. The `manifest/base` is geared toward the per-namespace Deployment model.

## Things you almost certainly want to change

1. **`REDPANDA_BROKER`** — point at your Kafka-compatible broker. The default is `localhost:9092` which is only useful for local development.
2. **`FINDINGS_TOPIC`** — pick a topic name that matches your consumer.
3. **Custom-resource Role rules** — if you want the monitor to list specific CRDs (for example Flux or cert-manager), add namespaced `get`, `list`, and `watch` rules for those API groups/resources in your overlay.
4. **Metrics ingress** — the base `NetworkPolicy` allows any pod in the cluster to scrape `:8080/metrics`. To restrict scraping to a specific namespace or label, patch the `ingress[0].from` field in your overlay.
5. **Optional cluster discovery** — most clusters allow authenticated API discovery without extra RBAC. If yours does not, include `manifest/overlays/cluster-discovery` or copy its `ClusterRole`/`ClusterRoleBinding`. It grants only discovery endpoint access, not cross-namespace resource reads.
