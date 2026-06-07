# Performance & resource-usage improvements

A tracked backlog of resource-usage improvements for the monitor, from a
performance review of `main`. Each item notes impact, effort, and status.

**Context:** for a typical namespace the monitor is already cheap — one poll
every 60s, bounded state, package-level regexes, controlled metric cardinality.
The wins below are (1) one real OOM-safety gap, (2) wasted work that bites
busy/large namespaces, and (3) an architectural ceiling that only matters if the
monitor is fanned out across many namespaces.

Status legend: ✅ done · ⏳ planned · 🔭 deferred

---

## Tier 1 — Quick wins (low effort, real value)

### 1. ✅ Set `GOMEMLIMIT`
The container sets `limits.memory` but the Go runtime does not read the cgroup
memory limit, so a transient spike (a huge List, an event flood) can push the
heap past the limit and the kernel OOM-kills the pod. A soft limit (~90% of the
hard limit) makes the GC reclaim aggressively before that happens.

- **Impact:** removes a real OOMKill class; smoother memory profile.
- **Effort:** trivial.
- **Shipped:** `GOMEMLIMIT=115MiB` in `chart` (`runtime.goMemLimit`, default
  `115MiB`) and `manifest/base/deployment.yaml`. Keep it at ~90% of
  `resources.limits.memory`.
- **Note:** `GOMAXPROCS` is already handled — Go 1.25+ reads the cgroup CPU
  quota, so with `limits.cpu: 100m` it auto-rounds to `GOMAXPROCS=1`.

### 2. ✅ Stop marshaling the whole state every poll just for a gauge
`updateStateMetrics()` serialized the entire state (`json.Marshal`, up to
~900 KB) on every poll — including idle polls — only to set the `state_bytes`
gauge, which `maybeSave` already sets on write.

- **Impact:** eliminates one full JSON marshal + allocation per idle poll (the
  biggest steady-state cost when there's nothing to do).
- **Effort:** trivial.
- **Shipped:** `state_bytes` updates at write cadence (in `maybeSave`); the
  per-poll path now only sets the cheap `len()` count gauges.

### 3. ✅ Compact JSON for the ConfigMap
`Save`/`create` used `MarshalIndent`, inflating the payload with whitespace
(more etcd bytes, closer to the 1 MiB object ceiling, more watch fan-out) and
CPU — for a blob no human reads. It also kept the written payload **larger** than
the compact size measured by the `MAX_STATE_BYTES` guard, so the guard could pass
while the actual write exceeded the limit.

- **Impact:** ~20–30% smaller state object; the size guard now matches what is
  written.
- **Effort:** trivial.
- **Shipped:** `json.Marshal` (compact) in `state.go`.

### 4. ✅ Only list resources that enabled checks consume
`listSnapshot` listed all 8 resource types every poll regardless of which checks
were enabled.

- **Impact:** disabling checks now proportionally reduces per-poll API calls and
  decode work; zero cost when all checks are on.
- **Effort:** small.
- **Shipped:** each List in `snapshot.go` is gated on the consuming check(s); an
  enabled check still always triggers its List, so the `complete:false`-on-
  list-error behavior is preserved. Covered by
  `TestPollSkipsListsForDisabledChecks`.

### 5. ✅ Server-side field selector for events
`checkEvents` fetched all events then filtered to `type=Warning` in memory.
Events are the highest-churn object in a busy namespace.

- **Impact:** sheds the (often large) Normal-event payload at the API server; in
  a chatty namespace this can cut the events payload by an order of magnitude.
- **Effort:** small.
- **Shipped:** `FieldSelector: "type=Warning"` on the events List; the in-memory
  Warning filter remains as a safety net (fakes that ignore field selectors stay
  correct). Covered by `TestEventsListUsesWarningFieldSelector`.

---

## Tier 2 — Medium effort

### 6. ⏳ The state-size pruning is O(n²) in JSON marshals
`pruneForStateSize` calls `currentStateBytes()` — a full `json.Marshal` of the
whole state — once per pruning iteration. When state is over `MAX_STATE_BYTES`
and shedding hundreds of entries, that is hundreds of full marshals of a ~900 KB
structure: quadratic CPU/allocation, hitting exactly when state is largest.

- **Fix:** marshal once for the starting size, estimate per-entry cost (or prune
  by count using `size/len`), drop in a batch, then marshal once to confirm.
- **Impact:** turns a worst-case CPU spike into linear work.
- **Effort:** moderate; needs a test (the size guard is a correctness boundary —
  etcd 1 MiB).

### 7. ⏳ Strip `managedFields` from listed objects
Every List decodes full objects including `metadata.managedFields` (server-side
-apply bookkeeping the monitor never reads, often 30–50% of an object's JSON),
paid in decode CPU and resident memory every poll.

- **Fix:** a `cache.TransformFunc` that nils `managedFields` — cleanest if
  adopting informers (#9). Hard to do server-side on a raw List.
- **Impact:** meaningful memory/CPU reduction in pod-dense namespaces.
- **Effort:** small with informers, awkward without.

### 8. ⏳ Client QPS/Burst
`buildKubeClient` uses client-go defaults (QPS=5, Burst=10). A normal poll is
fine, but when `checkLogs` fans `GetLogs` across many unhealthy pods it can
exceed the client-side limiter and self-throttle, stretching poll duration
(visible as rising `poll_duration_seconds`).

- **Fix:** bump `restCfg.QPS`/`Burst` (e.g. 20/30) or cap log-scan concurrency.
- **Impact:** avoids self-throttling under incident fan-out.
- **Effort:** one-liner; gate on observed `poll_duration_seconds`.

---

## Tier 3 — Architectural

### 9. 🔭 Polling Lists → shared informers (watch-backed cache) — deferred
Re-LISTing and re-decoding everything every 60s is the biggest lever. A
namespace-scoped `SharedInformerFactory` would maintain a local cache via a
single watch per type; each poll reads the cache (no round-trips, no repeated
decode), cutting both the monitor's CPU/alloc and aggregate API-server load.

- **Trade-off:** informers keep the full object cache resident continuously
  (steady-state memory up, ~one List's working set), add persistent watch
  connections, and are a real rearchitecture that blurs the deliberately simple
  "stateless polling" design.
- **Decision:** **deferred.** Only pays off if the deployment story becomes
  "hundreds of these, cluster-wide." Revisit when `poll_duration_seconds`,
  `state_bytes`, or memory working set say it's needed. Pairs naturally with #7.

---

## History

- **2026-06-07:** Tier 1 (#1–5) implemented in `perf/tier1-resource-optimizations`.
  #9 explicitly deferred. Tier 2 (#6–8) remains planned.
