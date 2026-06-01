# manifest/

This directory contains a kustomize **base** for deploying `autonomous-monitor` to a single target namespace.

## What this is

- A minimal, generic set of Kubernetes resources (Namespace, ServiceAccount, ClusterRole, ClusterRoleBinding, Role, RoleBinding, Deployment, Service).
- Safe defaults: non-root, read-only root filesystem, dropped capabilities, `seccompProfile: RuntimeDefault`, liveness/readiness probes.
- The ClusterRole grants the minimum namespace-scoped reads required plus discovery of installed CRDs.
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
3. **`ClusterRole.apiGroups` for custom resources** — the base grants discovery on `apiextensions.k8s.io`. If you want the monitor to actually *list* specific CRDs (e.g. Flux, cert-manager), add those groups to the ClusterRole in your overlay.
4. **`ClusterRoleBinding`** — the base binds the cluster-scoped reads cluster-wide. If you want a tighter posture, replace this with per-namespace `RoleBinding`s that bind a tighter `Role` per target namespace.
