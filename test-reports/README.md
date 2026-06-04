# Test Report — autonomous-monitor

This folder documents the test suite, the rationale for each test, and
captures raw evidence of the most recent test run so a reviewer can
audit the results without re-running anything.

The test suite is small (66 top-level tests, ~4,000 LOC of production
code) and runs in under a second on a single core. The breakdown is
roughly:

- 30 check-family tests (`checks_test.go`) — one per check family, plus
  helpers and a few shared utility tests
- 16 monitor-state tests (`monitor_test.go`) — end-to-end poll logic
- 10 config tests (`config_test.go`) — env loading, defaults, the
  recent downstream-triage rename + deprecation alias
- 3 state-store tests (`state_test.go`) — load/save/conflict-retry
- 2 docs-contract tests (`docs_contract_test.go`) — guards the README
  against drift in metric names and config var names
- 4 publisher tests (`publisher_test.go`, `publisher_pure_test.go`) —
  interface contract, Finding JSON contract, and an in-process broker
  round-trip for the pure-Go Kafka backend

## How to read this report

| Section | What it answers |
|---|---|
| [Run summary](#run-summary) | How many tests, pass/fail counts, runtime, coverage, lint result, for the most recent run. |
| [Test catalogue](#test-catalogue) | Every test name, location, purpose, expected result, and actual result from the captured run. |
| [Evidence](#evidence) | The raw files a reviewer can open to verify the summary. |

## Run summary

Captured on **2026-06-04T20:46:37Z** against commit
**`b7b196d4abf2b4107012dbed07479e6968d0cdef`** on a **darwin/arm64**
host running **go1.26.3**.

### Default build (`confluent-kafka-go`)

| Metric | Value |
|---|---|
| Top-level tests | **64** |
| Pass / Fail / Skip | **64 / 0 / 0** |
| Total leaf-test runtime | **0.20s** |
| Average leaf-test runtime | **3.1ms** |
| Slowest test | `TestPollTimesOutSlowCheck` (200ms — intentionally sleeps to exercise the check-timeout path) |
| Statement coverage | **69.2%** |
| `golangci-lint run` | **0 issues** |

### Pure build (`twmb/franz-go`, build tag `kafka_pure`)

| Metric | Value |
|---|---|
| Top-level tests | **66** (adds 2 round-trip tests for the franz-go backend) |
| Pass / Fail / Skip | **66 / 0 / 0** |
| Total leaf-test runtime | **0.22s** |
| Average leaf-test runtime | **3.3ms** |
| Statement coverage | **72.1%** (the franz-go `publisher_pure.go` is now exercised by the kfake round-trip tests) |
| `golangci-lint run --build-tags kafka_pure` | **0 issues** |

Both build paths are exercised in CI on every push and pull request.
A merge that breaks either path blocks the release pipeline.

## Test catalogue

The catalogue groups tests by the file they live in. Within each
group, tests are listed in the order they appear in the source.
"Result" is the status captured by the most recent run; see
[`evidence/test-default.json`](./evidence/test-default.json) and
[`evidence/test-pure.json`](./evidence/test-pure.json) for the
machine-readable record.

### `finding.go` — Finding JSON contract (covered via `checks_test.go`)

These tests pin the public Finding JSON schema that consumers depend
on. The schema is a v1.0.0 stability commitment per
[`ROADMAP.md`](../ROADMAP.md); any change here needs a major version
bump and a CHANGELOG note.

| Test | Location | Purpose | Expected | Result |
|---|---|---|---|---|
| `TestNewFindingShape` | `checks_test.go:25` | A freshly-constructed `Finding` carries the right `source` / `payload_type` / identifiers, derives the deterministic `id` from the canonical `namespace\|kind\|name\|check\|reason` tuple, and stamps `Classification` and `Severity` from the score. `FirstSeen` is zero on a fresh finding; the monitor stamps it on first publish. | All fields equal the documented schema; `id` is reproducible from the same input. | **PASS** |
| `TestClassificationForScore` | `checks_test.go:54` | Pin the score→classification bands: `<20` healthy, `20–39` watch, `40–59` suspicious, `60–79` degraded, `≥80` incident-likely. | All 12 score values across all 5 bands return the expected class. | **PASS** |
| `TestSeverityForScore` | `checks_test.go:79` | Pin the score→severity bands: `<40` low, `40–59` medium, `60–79` high, `≥80` critical. | All 8 score values return the expected severity. | **PASS** |
| `TestClampScore` | `checks_test.go:100` | Scores outside `[0, 100]` are clamped at the boundaries; in-range scores are returned unchanged. | `-1 → 0`, `101 → 100`, `42 → 42`. | **PASS** |
| `TestFindingMarshalRoundTrip` | `publisher_test.go:41` | Marshaling a synthetic `Finding` produces a JSON object with every documented key, the right `source` / `payload_type` / `classification` / `severity`, and a 64-character sha256 hex `id`. This is the contract test for the wire format. | Every required key present; score-65 finding classifies as `degraded`/`high`; `id` is 64 hex chars. | **PASS** |

### `checks.go` — check-family logic

Each of the ten check families has at least one test below. The
helpers (`scoreWaitingReason`, `podReady`, `shouldScanLogs`, etc.)
also have tests so a regression in shared scoring logic fails fast
rather than slipping through one of the family tests.

| Test | Location | Purpose | Expected | Result |
|---|---|---|---|---|
| `TestScoreWaitingReason` | `checks_test.go:112` | The `container.waiting.reason` → numeric score mapping used by the pod-health check. `CrashLoopBackOff` is the worst (85); pull/config errors are next (75); `ContainerCreating` and `PodInitializing` are transient and score 0; `Pending` and unknown reasons land at the baseline 50. | All 11 reasons map to the documented score. | **PASS** |
| `TestScoreEventReason` | `checks_test.go:133` | The Kubernetes Event reason → numeric score mapping. FailedScheduling/FailedMount/FailedAttachVolume (75); BackOff/CrashLoopBackOff/Unhealthy/OOMKilled (70); generic Failed (65); unknown (50). | All 12 reasons map to the documented score. | **PASS** |
| `TestNormalizeReason` | `checks_test.go:155` | Reason strings are lowercased, separators (`_`, ` `, `/`) are collapsed to `-`, and CamelCase boundaries insert a `-`. Empty / whitespace input becomes the literal `"unknown"`. | All 11 cases produce the expected normalised form. | **PASS** |
| `TestPodReady` | `checks_test.go:176` | A `Pod` is "ready" iff it has a `PodReady` condition set to `ConditionTrue`. Missing condition or `ConditionFalse` both yield not-ready. | `Ready=True` → true; `Ready=False` → false; missing condition → false. | **PASS** |
| `TestEventLastSeen` | `checks_test.go:195` | The "when did this event last happen" derivation: prefer `LastTimestamp`, then `EventTime`, then `CreationTimestamp`. The fake client only populates a subset of these on different Events and we have to pick the right fallback each time. | All three sources are honoured in order. | **PASS** |
| `TestAppendMemSample` | `checks_test.go:211` | The rolling-window memory sampler. After more than `maxSamples` (5) pushes, oldest is evicted and newest is appended. | After 6 pushes the window has 5 elements, the first is the 2nd sample, the last is the 6th. | **PASS** |
| `TestMemoryTrending` | `checks_test.go:232` | The "is memory trending toward the limit" detector. Requires ≥3 samples and a delta of ≥10 between newest and oldest; the note string includes the start/end percentages. | nil/<3 samples/<10 delta/flat/declining → not trending; monotonic rise of 10+ → trending; the note names the percentages. | **PASS** |
| `TestFmtMiB` | `checks_test.go:260` | Bytes-to-human helper used in evidence strings. 512×1024 → `512Ki`, 128×1024×1024 → `128Mi`. | Both conversions match. | **PASS** |
| `TestIsBuiltinGroup` | `checks_test.go:269` | Distinguishes "always-skippable, scanned via the typed client" API groups from "maybe-interesting, scan via dynamic client" custom resources. | Builtins (`apps`, `batch`, `rbac.*`, `metrics.k8s.io`) return true; custom groups return false. | **PASS** |
| `TestContains` | `checks_test.go:284` | Tiny `[]string` membership helper. nil slice and missing element both return false. | All three cases match. | **PASS** |
| `TestShouldScanLogs` | `checks_test.go:296` | Decides whether the log-scan check should tail a pod's logs. Triggers on not-ready, container in CrashLoopBackOff, or restart count over the threshold. | Each of the four pod-state fixtures triggers or doesn't trigger as expected. | **PASS** |
| `TestDeploymentFindings` | `checks_test.go:325` | Three sub-tests on `deploymentFindings`: emits `available-replicas-below-desired`, emits `generation-lag` when `Generation > ObservedGeneration`, emits no findings for a healthy deployment. | 3/3 sub-tests produce the expected finding list. | **PASS** |
| `TestCheckPodsReportsCrashLoopBackOff` | `checks_test.go:365` | End-to-end pod check against a fake clientset: a `CrashLoopBackOff` container emits exactly one finding with `reason="crash-loop-back-off"` and `score=85`. | 1 finding, correct reason, score 85. | **PASS** |
| `TestCheckPodsReportsOOMKilled` | `checks_test.go:407` | Pod whose `LastTerminationState.Terminated.Reason == "OOMKilled"` emits a finding with `reason="oom-killed"`. | 1 finding, reason `oom-killed`. | **PASS** |
| `TestCheckResourceSpecsReportsMissingRequests` | `checks_test.go:446` | A pod with containers missing memory requests, memory limits, and CPU requests emits one finding per missing field. | 3 findings: `missing-memory-request`, `missing-memory-limit`, `missing-cpu-request`. | **PASS** |
| `TestCheckEventsEmitsWarningFinding` | `checks_test.go:481` | A Warning Event inside the lookback window, with reason `BackOff`, is converted to a finding. `MatchingKubernetesEventFound` is set true and reason normalises to `back-off`. | 1 finding, check `event`, `MatchingKubernetesEventFound=true`, reason `back-off`. | **PASS** |
| `TestCheckEventsSkipsEventsOutsideLookback` | `checks_test.go:516` | An old Warning Event (2h old, lookback 30m) is not converted to a finding. | 0 findings. | **PASS** |
| `TestCheckWorkloadsReportsStatefulSetBelowDesired` | `checks_test.go:538` | StatefulSet with 3 desired / 1 ready emits `ready-replicas-below-desired`. | 1 finding, correct reason. | **PASS** |
| `TestCheckServicesReportsSelectorWithNoPods` | `checks_test.go:555` | Service whose selector matches zero pods in the namespace emits `selector-matches-no-pods`. | 1 finding, check `service`, reason `selector-matches-no-pods`. | **PASS** |
| `TestCheckServicesSkipsSelectorWithMatchingPod` | `checks_test.go:580` | Service whose selector does match a labelled pod emits no finding (no false positive). | 0 findings. | **PASS** |
| `TestCheckServicesReportsPendingLoadBalancer` | `checks_test.go:606` | A `Service` of type `LoadBalancer` with no `Ingress` status emits `load-balancer-pending`. | 1 finding, reason `load-balancer-pending`. | **PASS** |
| `TestCheckPVCsReportsPendingAndLostClaims` | `checks_test.go:628` | A `Pending` and a `Lost` PVC each emit one finding. | 2 findings with reasons `pending` and `lost`. | **PASS** |
| `TestCheckPVCsReportsTrueConditions` | `checks_test.go:652` | A `Bound` PVC with a `FileSystemResizePending=True` condition emits `condition-file-system-resize-pending-true`. | 1 finding, correct reason. | **PASS** |
| `TestCheckScalingReportsHPAConditionsAndReplicaPressure` | `checks_test.go:676` | An HPA with `ScalingActive=False`, `ScalingLimited=True`, and `CurrentReplicas == MaxReplicas` emits three findings: `scaling-active-false`, `scaling-limited`, `max-replicas-reached`. | All 3 reasons present. | **PASS** |
| `TestCheckCustomResourcesReportsGenerationLag` | `checks_test.go:706` | A `HelmRelease`-shaped custom resource with `generation=5, observedGeneration=3` emits `generation-lag`. | 1 finding, reason `generation-lag`. | **PASS** |
| `TestCheckCustomResourcesReportsFalseCondition` | `checks_test.go:749` | A custom resource with a `Ready=False` condition (no generation lag) emits `condition-ready-false`. | 1 finding, reason `condition-ready-false`. | **PASS** |
| `TestCustomResourceAllowlistAndExcludelist` | `checks_test.go:792` | The CR allowlist semantics: a group-only entry matches every resource in the group; a `group/version/resource` entry matches one resource; an excludelist entry vetoes a previously-allowed resource. | source-toolkit group → allowed; helm-toolkit/v2/helmreleases → allowed; noisy.example.com/widgets → denied (excluded). | **PASS** |
| `TestCustomResourceDiscoveryIsCached` | `checks_test.go:812` | API discovery results are cached for `CustomResourceDiscoveryTTL`. The second call inside the TTL window returns the cached list even if the fake client's `Resources` was wiped between calls. | Both calls return the originally-discovered list of length 1. | **PASS** |
| `TestCheckResourceUsageIncrementsSampleMetric` | `checks_test.go:840` | The `autonomous_monitor_resource_samples_total{namespace,backend}` counter is incremented by exactly 1 per `checkResourceUsage` call. The fake metrics client returns 50Mi used on a 100Mi-limit pod. | Counter delta = 1. | **PASS** |
| `TestIsSuppressed` | `checks_test.go:932` | The `SUPPRESS_CONFIGMAP` key matching: exact `kind/name/reason` keys, two-component wildcards (`kind/*/reason`, `*/name/*`), and one-component wildcards (`*/*/reason`, `kind/*/*`). | 7 positive + 3 negative cases all resolve correctly. | **PASS** |
| `TestIsSuppressedEmptyMap` | `checks_test.go:966` | Empty (or nil) suppression set is a no-op, never a blanket "suppress everything". | `isSuppressed` returns false. | **PASS** |

### `config.go` — environment loading

These tests are hermetic — they use `t.Setenv` so each test runs
with a clean environment regardless of what's in the developer's
shell. The `clearConfigEnv` helper unsets the whole known keyspace.

| Test | Location | Purpose | Expected | Result |
|---|---|---|---|---|
| `TestBoolEnvDefaultOn` | `config_test.go:10` | The bool parser used for every `CHECK_*_ENABLED` flag: empty, `1`, `true`, `yes`, `on`, `enabled` all mean on; `0`, `false`, `no`, `off`, `disabled` all mean off. | Each value parses to the right side. | **PASS** |
| `TestLoadConfigDefaults` | `config_test.go:32` | With no env vars set, the config picks up every default documented in the README: 60s poll, `localhost:9092` broker, `k8s.namespace.findings` topic, 30m upstream cooldown, 5000/2000 caps, 900KiB state, and so on. Also asserts that no CR allow/exclude list is set by default. | Every default matches the README table. | **PASS** |
| `TestLoadConfigCheckTimeout` | `config_test.go:80` | `CHECK_TIMEOUT` accepts any `time.ParseDuration` string and is wired into the per-check context. | `CHECK_TIMEOUT=250ms` → `cfg.CheckTimeout == 250ms`. | **PASS** |
| `TestResourceBackendDisabledDisablesUsageCheck` | `config_test.go:91` | `RESOURCE_USAGE_BACKEND=disabled` is the kill switch for the metrics-client wiring. Even if `CHECK_RESOURCE_USAGE_ENABLED` is left at the default (`true`), the check is forced off and the metrics client is never constructed in `main.go`. | `cfg.Checks.ResourceUsage == false`. | **PASS** |
| `TestLoadConfigCustomResourceControls` | `config_test.go:100` | The four CR knobs (`CUSTOM_RESOURCE_ALLOWLIST`, `CUSTOM_RESOURCE_EXCLUDELIST`, `CUSTOM_RESOURCE_DISCOVERY_TTL`) are loaded, trimmed, lowercased, and the TTL is parsed as a duration. | Allowlist, excludelist, and TTL match the values passed in. | **PASS** |
| `TestLoadConfigCustomResourceGroupAliases` | `config_test.go:118` | `CUSTOM_RESOURCE_GROUPS` is a documented alias for `CUSTOM_RESOURCE_ALLOWLIST`; same for the `_EXCLUDE_GROUPS` pair. Useful for users who learned the old name. | The alias populates the canonical field. | **PASS** |
| `TestLoadConfigDownstreamTriageDefaults` | `config_test.go:132` | With no env vars set, the new `DOWNSTREAM_*` knobs pick up the documented defaults (`true`, `60`, `30m`, `10m`). | All four values match. | **PASS** |
| `TestLoadConfigDownstreamTriageExplicitEnv` | `config_test.go:149` | Setting `DOWNSTREAM_*` explicitly overrides the default. | All four values match what was set. | **PASS** |
| `TestLoadConfigDownstreamTriageDeprecatedAlias` | `config_test.go:171` | The legacy `AI_*` names still work as a deprecation alias. Existing deployments with `AI_TRIAGE_ENABLED=false` etc. keep working. | All four values are read from the legacy names. | **PASS** |
| `TestLoadConfigDownstreamTriageNewOverridesDeprecated` | `config_test.go:193` | If both the new and the legacy name are set, the new name wins. This is the rule every well-behaved deprecation alias needs. | `DownstreamTriageEnabled == true`, `DownstreamMinScore == 88`. | **PASS** |

### `state.go` — state ConfigMap persistence

| Test | Location | Purpose | Expected | Result |
|---|---|---|---|---|
| `TestStateStoreCreatesMissingConfigMap` | `state_test.go:17` | First-run behaviour: there is no state ConfigMap, so `Load` returns a fresh in-memory state and creates the ConfigMap with the serialised `state.json` data. | `Load` returns no error; the ConfigMap exists; `data["state.json"]` is non-empty. | **PASS** |
| `TestStateStoreAdoptsExistingConfigMap` | `state_test.go:38` | Subsequent-run behaviour: a pre-existing ConfigMap is loaded verbatim, including the prior findings and observations. Verifies the monitor does not silently reset state on restart. | The adopted state's `Findings` has the one finding that was serialised in the fixture. | **PASS** |
| `TestStateStoreRetriesUpdateConflict` | `state_test.go:62` | The state store has to handle Optimistic Concurrency Control: if two writes race, one of them gets a `Conflict` from the API server and has to retry. The fake client is rigged to return a conflict on the first `update`, then succeed. | `Save` returns no error; the reactor's conflict counter is 1. | **PASS** |

### `monitor.go` — poll orchestration, dedup, retry, prune

These tests exercise the monitor end-to-end against a fake clientset.
A `recordingPublisher` (in `monitor_test.go`) plays the role of the
Kafka backend, recording what was published so the test can assert
on the Finding stream.

| Test | Location | Purpose | Expected | Result |
|---|---|---|---|---|
| `TestMonitorPublishesNotReadyFindingWithAIDispatchAndCooldown` | `monitor_test.go:31` | The first poll of a not-ready pod publishes exactly one finding with `AITriageRequired=true` and a non-nil `CooldownUntil`. The second poll must NOT re-publish (cooldown active). | 1 finding after first poll; still 1 finding after second poll. | **PASS** |
| `TestMonitorRetriesFailedFindingPublish` | `monitor_test.go:59` | When the publisher returns an error, the state records `LastPublishFailed` and does not start the AI cooldown. When the publisher recovers on the next poll, the finding is re-published, `LastPublished` is set, `LastPublishFailed` is cleared, the AI cooldown starts, and `AITriageRequired` is still true. | 1 publish attempt → failed state; 2 publish attempts → success, cooldown set, AI flag true. | **PASS** |
| `TestMonitorPublishesResolvedFinding` | `monitor_test.go:105` | When a previously-ongoing finding no longer matches a poll, the monitor publishes a synthetic finding with `status="resolved"`, `score=0`, and `classification="healthy"`. | 2 findings total: the original ongoing and a resolved one. | **PASS** |
| `TestMonitorRetriesFailedResolvedPublish` | `monitor_test.go:130` | A failed resolved publish does NOT mark the state as resolved — the state stays `ongoing` with `LastPublishFailed` set. When the publish succeeds on the next poll, the state is flipped to `resolved` and a third publish (the success) is recorded. | 2 publish attempts → still ongoing + failed; 3 publish attempts → resolved. | **PASS** |
| `TestResourceSpecsRunsWhenPodHealthDisabled` | `monitor_test.go:176` | The 10 check families are independent. Toggling `CHECK_PODS_ENABLED=false` must not prevent `CHECK_RESOURCE_SPECS_ENABLED=true` from running. Catches a class of regression where a refactor accidentally gates one check on another. | Resource-spec findings are emitted; no `pod-health` findings appear. | **PASS** |
| `TestConfiguredChecksAreWiredIntoPoll` | `monitor_test.go:202` | Three sub-tests (`services`, `pvcs`, `scaling`) — each one with only that family enabled in `CheckConfig`, plus a fake object of the right kind, must produce a finding from that family. Catches the regression of adding a new check family but forgetting to wire it into `Monitor.Poll`. | For each sub-test, at least one finding carries the expected `check` value. | **PASS** |
| `TestPollTimesOutSlowCheck` | `monitor_test.go:286` | A check family that blocks beyond `CHECK_TIMEOUT` is cancelled and `complete=false` is reported. The poll itself returns within the timeout (not blocked on the slow check). The slow goroutine exits within a second. | Poll elapsed ≤ 100ms; no findings from the timed-out check; the slow reactor goroutine has exited. | **PASS** (200ms — this is the slowest test in the suite by design) |
| `TestMonitorExpiresOldResolvedFindings` | `monitor_test.go:330` | A resolved finding older than `ResolvedFindingRetention` is removed from the in-memory state on the next poll. | The old finding is no longer in `monitor.state.Findings`. | **PASS** |
| `TestMonitorPrunesStaleObservations` | `monitor_test.go:363` | An observation (restart counter, memory sample) whose `LastSeen` is older than `ObservationRetention` is pruned. A recent one is kept. | `old` is gone; `recent` is still there. | **PASS** |
| `TestMonitorPrunesObservationCapOldestFirst` | `monitor_test.go:389` | When the observation count exceeds `MaxObservations`, the oldest entries are dropped first (FIFO by `LastSeen`). | The oldest observation is gone; the cap is honoured. | **PASS** |
| `TestMonitorPrunesResolvedFindingsBeforeOngoing` | `monitor_test.go:416` | When `MaxFindings` is exceeded, resolved findings are pruned before ongoing ones — the monitor protects its in-flight state at the cost of history. | The oldest resolved finding is gone; the ongoing finding is untouched. | **PASS** |
| `TestMonitorRefusesOversizedStateAfterPruning` | `monitor_test.go:443` | Even after pruning, if the serialised state would still exceed `MaxStateBytes`, the monitor refuses to write and increments the `too-large` metric. Verifies the safety net that prevents a runaway state from breaking the ConfigMap limit (1 MiB by default in etcd). | `lastWrite` is zero; no `Save` was issued. | **PASS** |

### `docs_contract_test.go` — README ↔ code drift guards

These tests are not "behaviour" tests; they are invariant guards.
They fail the build the moment the README and the code disagree, so
the documentation can never silently drift.

| Test | Location | Purpose | Expected | Result |
|---|---|---|---|---|
| `TestReadmeDocumentsImplementedMetrics` | `docs_contract_test.go:11` | Every `autonomous_monitor_*` metric declared in `metrics.go` must be documented in the README's Metrics section, and the README must not document metrics that don't exist. | Documented set == implemented set. | **PASS** |
| `TestReadmeDocumentsImplementedConfigVariables` | `docs_contract_test.go:21` | Every env var of the form `FOO_BAR` referenced in `config.go` must be documented in the README's Configuration section, and vice versa. | Documented set == implemented set. | **PASS** |

### Publisher

The Publisher contract is split across two files. The hermetic tests
in `publisher_test.go` run under every build tag; the `kfake`-backed
round-trip tests in `publisher_pure_test.go` are guarded behind
`-tags kafka_pure` because the default backend can't easily run
without a real broker.

| Test | Location | Purpose | Expected | Result |
|---|---|---|---|---|
| `TestPublisherInterfaceContract` | `publisher_test.go:29` | Compile-time + reflection check: the `Publisher` interface declares exactly `PublishFinding(context.Context, Finding) error` and `Close()`, and the test's `recordingPublisher` (the one monitor tests use) implements it. | `reflect.TypeOf(recordingPublisher).Implements(publisherInterfaceType)` is true. | **PASS** |
| `TestFindingMarshalRoundTrip` | `publisher_test.go:41` | See catalogue under `finding.go` above. | (same) | **PASS** |
| `TestKafkaPublisherRoundTripPure` | `publisher_pure_test.go:19` | End-to-end: build a finding, publish it through the pure-Go Kafka publisher, consume it back with a fresh franz-go client against an in-process `kfake` cluster, and assert the bytes round-trip. | 1 record on the topic; `ID`, `Score`, and `Classification` match what was sent. | **PASS** (pure build only) |
| `TestKafkaPublisherReportsDeliveryTimeout` | `publisher_pure_test.go:96` | With a 1ms delivery timeout against a real (kfake) broker and a context that allows 200ms, the publisher still surfaces an error. Confirms the `PublishTimeout` knob is honoured and does not silently mask a broker outage. | `PublishFinding` returns a non-nil error. | **PASS** (pure build only) |

## Evidence

Everything below is captured verbatim from the most recent run. These
files are the primary source of truth for the summary in
[Run summary](#run-summary); a reviewer can open them in any text
editor and audit the run.

### Raw test output

| File | Format | What's in it |
|---|---|---|
| [`evidence/test-default.txt`](./evidence/test-default.txt) | Human-readable verbose `go test` output for the default build (64 top-level tests, 0 failures). | Each `=== RUN` / `--- PASS` / `--- FAIL` line, the `ok` summary at the end. |
| [`evidence/test-pure.txt`](./evidence/test-pure.txt) | Same, for `-tags kafka_pure` (66 top-level tests, 0 failures). | Same. |
| [`evidence/test-default.json`](./evidence/test-default.json) | `go test -json` machine-readable stream for the default build. | One JSON object per test event (`run` / `output` / `pass` / `fail`). |
| [`evidence/test-pure.json`](./evidence/test-pure.json) | Same, for `-tags kafka_pure`. | Same. |
| [`evidence/summary-default.tsv`](./evidence/summary-default.tsv) | Tab-separated: `result   elapsed_seconds   test_name`, default build. | Filter with `awk -F'\t' '$1=="pass"' summary-default.tsv` to list passing tests with their timing. |
| [`evidence/summary-pure.tsv`](./evidence/summary-pure.tsv) | Same, for `-tags kafka_pure`. | Same. |

### Coverage

| File | Format | What's in it |
|---|---|---|
| [`evidence/coverage-default.out`](./evidence/coverage-default.out) | `go tool cover` profile (default build). | One record per statement; feed to `go tool cover -html=...` for an HTML report. |
| [`evidence/coverage-default.txt`](./evidence/coverage-default.txt) | `go tool cover -func` output (default build). | Per-function coverage table; the trailing `total:` line is the headline number (69.2%). |
| [`evidence/coverage-pure.out`](./evidence/coverage-pure.out) | Same, for `-tags kafka_pure`. | Same. |
| [`evidence/coverage-pure.txt`](./evidence/coverage-pure.txt) | Same, for `-tags kafka_pure`. | Same. (72.1%) |

The coverage gap is concentrated in the Kafka publisher backends
(the confluent impl is exercised only by integration smoke tests, the
franz-go impl by the kfake round-trip), the `metrics.go` Prometheus
registration (covered indirectly), and the per-poll code paths that
require a real Kubernetes client. The integration gap is the
largest item in [`ROADMAP.md`](../ROADMAP.md) under "Pre-1.0.0 work".

### Lint

| File | What's in it |
|---|---|
| [`evidence/lint-default.txt`](./evidence/lint-default.txt) | `golangci-lint run` output for the default build. Contains the single line `0 issues.` |
| [`evidence/lint-pure.txt`](./evidence/lint-pure.txt) | `golangci-lint run --build-tags kafka_pure` output. Same. |

The linter configuration (`.golangci.yml`) is committed and runs in
CI as a gating step before tests.

## How to reproduce this report

```bash
# 1. Run the default-build test suite, capture verbose + JSON output
go test -race -count=1 -v             ./... > test-reports/evidence/test-default.txt 2>&1
go test -race -count=1 -v -json        ./... > test-reports/evidence/test-default.json 2>&1
CGO_ENABLED=1 go test -race -count=1 -coverprofile=test-reports/evidence/coverage-default.out ./...
go tool cover -func=test-reports/evidence/coverage-default.out > test-reports/evidence/coverage-default.txt

# 2. Same, for the pure-Go Kafka build path
go test -race -count=1 -v             -tags 'kafka_pure' ./... > test-reports/evidence/test-pure.txt 2>&1
go test -race -count=1 -v -json        -tags 'kafka_pure' ./... > test-reports/evidence/test-pure.json 2>&1
go test -race -count=1 -tags 'kafka_pure' -coverprofile=test-reports/evidence/coverage-pure.out ./...
go tool cover -func=test-reports/evidence/coverage-pure.out > test-reports/evidence/coverage-pure.txt

# 3. Lint both paths
golangci-lint run                                > test-reports/evidence/lint-default.txt 2>&1
golangci-lint run --build-tags kafka_pure        > test-reports/evidence/lint-pure.txt 2>&1

# 4. Regenerate the summary TSV files
jq -r 'select((.Action=="pass" or .Action=="fail" or .Action=="skip") and (.Test | type == "string") and (.Test | contains("/") | not)) | "\(.Action)\t\(.Elapsed // 0)\t\(.Test)"' test-reports/evidence/test-default.json > test-reports/evidence/summary-default.tsv
jq -r 'select((.Action=="pass" or .Action=="fail" or .Action=="skip") and (.Test | type == "string") and (.Test | contains("/") | not)) | "\(.Action)\t\(.Elapsed // 0)\t\(.Test)"' test-reports/evidence/test-pure.json   > test-reports/evidence/summary-pure.tsv
```

The script writes over the previous evidence; check the resulting
`*.txt` / `*.json` / `*.tsv` files into git if you want the report
to reflect the new run.
