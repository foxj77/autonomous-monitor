package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	metricsv1beta1client "k8s.io/metrics/pkg/client/clientset/versioned"
)

type Monitor struct {
	cfg                            Config
	kube                           kubernetes.Interface
	metrics                        metricsv1beta1client.Interface
	dynamic                        dynamic.Interface
	publisher                      Publisher
	store                          *StateStore
	state                          *MonitorState
	health                         *PollHealth
	suppressions                   map[string]struct{} // loaded from SUPPRESS_CONFIGMAP each poll
	dirty                          bool
	lastWrite                      time.Time
	customResourceDiscovery        []*metav1.APIResourceList
	customResourceDiscoveryExpires time.Time
}

func NewMonitor(cfg Config, kube kubernetes.Interface, metricsClient metricsv1beta1client.Interface, dynClient dynamic.Interface, publisher Publisher, store *StateStore, state *MonitorState, health *PollHealth) *Monitor {
	return &Monitor{
		cfg:       cfg,
		kube:      kube,
		metrics:   metricsClient,
		dynamic:   dynClient,
		publisher: publisher,
		store:     store,
		state:     state,
		health:    health,
	}
}

func (m *Monitor) Poll(ctx context.Context) {
	m.loadSuppressions(ctx)
	start := time.Now()
	now := start.UTC()
	active := map[string]Finding{}
	complete := true

	// Build the shared snapshot once: every redundantly-listed resource is
	// listed a single time here, on the main goroutine, and the prior
	// observations checks depend on are value-copied. Checks read this snapshot
	// and never touch m.state, so a timed-out (abandoned) check goroutine can
	// never race the main goroutine's reads/writes of m.state.
	snap := m.buildSnapshot(ctx, now)

	if m.cfg.Checks.Pods {
		result := m.runCheck(ctx, "pods", func(checkCtx context.Context) checkResult {
			return m.checkPods(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.ResourceSpecs {
		result := m.runCheck(ctx, "resource-spec", func(checkCtx context.Context) checkResult {
			return m.checkResourceSpecs(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.Events {
		result := m.runCheck(ctx, "events", func(checkCtx context.Context) checkResult {
			return m.checkEvents(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.Workloads {
		result := m.runCheck(ctx, "workloads", func(checkCtx context.Context) checkResult {
			return m.checkWorkloads(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.Services {
		result := m.runCheck(ctx, "services", func(checkCtx context.Context) checkResult {
			return m.checkServices(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.PVCs {
		result := m.runCheck(ctx, "pvcs", func(checkCtx context.Context) checkResult {
			return m.checkPVCs(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.Scaling {
		result := m.runCheck(ctx, "scaling", func(checkCtx context.Context) checkResult {
			return m.checkScaling(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.Logs {
		result := m.runCheck(ctx, "logs", func(checkCtx context.Context) checkResult {
			return m.checkLogs(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.ResourceUsage {
		result := m.runCheck(ctx, "resource-usage", func(checkCtx context.Context) checkResult {
			return m.checkResourceUsage(checkCtx, snap)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}
	if m.cfg.Checks.CustomResource {
		// Refresh discovery on the main goroutine so the discovery cache is
		// never mutated from a check goroutine that may outlive the timeout.
		apiLists, discoveryErr := m.customResourceAPILists()
		result := m.runCheck(ctx, "custom-resources", func(checkCtx context.Context) checkResult {
			return m.checkCustomResources(checkCtx, apiLists, discoveryErr)
		})
		complete = complete && result.complete
		m.applyResult(ctx, result, now, active)
	}

	if complete {
		m.resolveMissingFindings(ctx, now, active)
	}

	m.expireResolvedFindings(now)
	m.pruneState(now)
	m.updateActiveMetrics(active)
	m.maybeSave(ctx, now)
	pollDuration.WithLabelValues(m.cfg.Namespace).Observe(time.Since(start).Seconds())
	if m.health != nil {
		m.health.MarkPollComplete()
	}
}

func (m *Monitor) runCheck(ctx context.Context, name string, fn func(context.Context) checkResult) checkResult {
	if m.cfg.CheckTimeout <= 0 {
		return fn(ctx)
	}

	checkCtx, cancel := context.WithTimeout(ctx, m.cfg.CheckTimeout)
	defer cancel()

	done := make(chan checkResult, 1)
	go func() {
		done <- fn(checkCtx)
	}()

	select {
	case result := <-done:
		if !result.complete && checkCtx.Err() == context.DeadlineExceeded {
			m.recordCheckTimeout(name)
		}
		return result
	case <-checkCtx.Done():
		if checkCtx.Err() == context.DeadlineExceeded {
			m.recordCheckTimeout(name)
		}
		return checkResult{complete: false}
	}
}

func (m *Monitor) recordCheckTimeout(name string) {
	checksTotal.WithLabelValues(m.cfg.Namespace, name, "timeout").Inc()
	log.Printf("ERROR: check timed out namespace=%s check=%s timeout=%s", m.cfg.Namespace, name, m.cfg.CheckTimeout)
}

// applyResult merges a completed check's observation deltas into m.state and
// then collects its findings. This runs only on the main goroutine, after
// runCheck has returned the result via its channel; the abandoned-goroutine
// timeout path returns an empty checkResult, so its observations are dropped.
func (m *Monitor) applyResult(ctx context.Context, result checkResult, now time.Time, active map[string]Finding) {
	for key, obs := range result.observations {
		copied := obs
		m.state.Observations[key] = &copied
		m.dirty = true
	}
	m.collectFindings(ctx, result.findings, now, active)
}

func (m *Monitor) collectFindings(ctx context.Context, findings []Finding, now time.Time, active map[string]Finding) {
	for _, finding := range findings {
		if m.isSuppressed(finding) {
			log.Printf("suppressed finding kind=%s name=%s reason=%s", finding.Kind, finding.Name, finding.Reason)
			continue
		}

		finding.LastSeen = now
		finding.Status = "ongoing"

		state, exists := m.state.Findings[finding.ID]
		if !exists {
			state = &FindingState{
				Kind:           finding.Kind,
				Name:           finding.Name,
				Check:          finding.Check,
				Reason:         finding.Reason,
				FirstSeen:      now,
				Status:         "ongoing",
				Classification: finding.Classification,
				Score:          finding.Score,
			}
			m.state.Findings[finding.ID] = state
			m.dirty = true
		}

		finding.FirstSeen = state.FirstSeen
		shouldPublish := !exists ||
			state.Status == "resolved" ||
			state.LastPublished == nil ||
			state.LastPublishFailed != nil ||
			finding.Score > state.Score ||
			finding.Classification != state.Classification
		var aiDispatchRequested bool
		var nextCooldownUntil *time.Time

		if m.cfg.DownstreamTriageEnabled && finding.Score >= m.cfg.DownstreamMinScore {
			if state.CooldownUntil == nil || now.After(*state.CooldownUntil) {
				cooldown := m.cfg.DownstreamCooldown
				if finding.Classification == "incident-likely" && m.cfg.DownstreamCooldownIncident > 0 {
					cooldown = m.cfg.DownstreamCooldownIncident
				}
				cooldownUntil := now.Add(cooldown)
				nextCooldownUntil = &cooldownUntil
				finding.AITriageRequired = true
				finding.CooldownUntil = &cooldownUntil
				aiDispatchRequested = true
				shouldPublish = true
			} else {
				finding.CooldownUntil = state.CooldownUntil
				aiCooldowns.WithLabelValues(m.cfg.Namespace).Inc()
			}
		}

		state.Kind = finding.Kind
		state.Name = finding.Name
		state.Check = finding.Check
		state.Reason = finding.Reason
		state.LastSeen = now
		state.Score = finding.Score
		state.Classification = finding.Classification
		state.Status = "ongoing"
		m.dirty = true

		if shouldPublish {
			if err := m.publish(ctx, finding); err != nil {
				state.LastPublishFailed = &now
				m.dirty = true
				active[finding.ID] = finding
				continue
			}
			state.LastPublished = &now
			state.LastPublishFailed = nil
			if aiDispatchRequested {
				state.LastAIDispatchRequested = &now
				state.CooldownUntil = nextCooldownUntil
				aiDispatchRequests.WithLabelValues(m.cfg.Namespace, "requested").Inc()
			}
			m.dirty = true
		}
		active[finding.ID] = finding
	}
}

func (m *Monitor) resolveMissingFindings(ctx context.Context, now time.Time, active map[string]Finding) {
	for id, state := range m.state.Findings {
		if state.Status != "ongoing" {
			continue
		}
		if _, ok := active[id]; ok {
			continue
		}
		resolved := Finding{
			Source:         sourceName,
			PayloadType:    payloadType,
			ID:             id,
			Namespace:      m.cfg.Namespace,
			Kind:           state.Kind,
			Name:           state.Name,
			Severity:       "low",
			Classification: "healthy",
			Score:          0,
			Check:          state.Check,
			Reason:         state.Reason,
			Status:         "resolved",
			FirstSeen:      state.FirstSeen,
			LastSeen:       now,
			Evidence:       []string{"deterministic check no longer reports this finding"},
		}
		if err := m.publish(ctx, resolved); err != nil {
			state.LastPublishFailed = &now
			m.dirty = true
			continue
		}
		state.Status = "resolved"
		state.LastSeen = now
		state.Score = 0
		state.Classification = "healthy"
		state.LastPublished = &now
		state.LastPublishFailed = nil
		m.dirty = true
	}
}

func (m *Monitor) expireResolvedFindings(now time.Time) {
	if m.cfg.ResolvedFindingRetention <= 0 {
		return
	}
	for id, state := range m.state.Findings {
		if state.Status != "resolved" {
			continue
		}
		if now.Sub(state.LastSeen) <= m.cfg.ResolvedFindingRetention {
			continue
		}
		delete(m.state.Findings, id)
		m.dirty = true
	}
}

func (m *Monitor) pruneState(now time.Time) {
	m.pruneStaleObservations(now)
	m.pruneObservationCap()
	m.pruneFindingCap()
	m.updateStateMetrics()
}

func (m *Monitor) pruneStaleObservations(now time.Time) {
	if m.cfg.ObservationRetention <= 0 {
		return
	}
	for key, obs := range m.state.Observations {
		if obs == nil || obs.LastSeen.IsZero() {
			continue
		}
		if now.Sub(obs.LastSeen) <= m.cfg.ObservationRetention {
			continue
		}
		delete(m.state.Observations, key)
		statePrunes.WithLabelValues(m.cfg.Namespace, "observation", "retention").Inc()
		m.dirty = true
	}
}

func (m *Monitor) pruneObservationCap() {
	if m.cfg.MaxObservations <= 0 || len(m.state.Observations) <= m.cfg.MaxObservations {
		return
	}
	items := make([]stateObservationItem, 0, len(m.state.Observations))
	for key, obs := range m.state.Observations {
		var lastSeen time.Time
		if obs != nil {
			lastSeen = obs.LastSeen
		}
		items = append(items, stateObservationItem{key: key, lastSeen: lastSeen})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].lastSeen.Before(items[j].lastSeen)
	})
	toDelete := len(m.state.Observations) - m.cfg.MaxObservations
	for i := 0; i < toDelete; i++ {
		delete(m.state.Observations, items[i].key)
		statePrunes.WithLabelValues(m.cfg.Namespace, "observation", "cap").Inc()
	}
	m.dirty = true
}

func (m *Monitor) pruneFindingCap() {
	if m.cfg.MaxFindings <= 0 || len(m.state.Findings) <= m.cfg.MaxFindings {
		return
	}
	pruned := m.pruneFindingsByStatus("resolved", len(m.state.Findings)-m.cfg.MaxFindings, "cap-resolved")
	if len(m.state.Findings) <= m.cfg.MaxFindings {
		if pruned > 0 {
			m.dirty = true
		}
		return
	}
	m.pruneFindingsByStatus("", len(m.state.Findings)-m.cfg.MaxFindings, "cap")
	m.dirty = true
}

func (m *Monitor) pruneFindingsByStatus(status string, count int, reason string) int {
	if count <= 0 {
		return 0
	}
	items := make([]stateFindingItem, 0, len(m.state.Findings))
	for id, finding := range m.state.Findings {
		if finding == nil {
			items = append(items, stateFindingItem{id: id})
			continue
		}
		if status != "" && finding.Status != status {
			continue
		}
		items = append(items, stateFindingItem{id: id, lastSeen: finding.LastSeen})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].lastSeen.Before(items[j].lastSeen)
	})
	if count > len(items) {
		count = len(items)
	}
	for i := 0; i < count; i++ {
		delete(m.state.Findings, items[i].id)
		statePrunes.WithLabelValues(m.cfg.Namespace, "finding", reason).Inc()
	}
	return count
}

func (m *Monitor) pruneForStateSize(maxBytes int) (int, error) {
	if maxBytes <= 0 {
		return m.currentStateBytes()
	}
	size, err := m.currentStateBytes()
	if err != nil {
		return 0, err
	}
	if size <= maxBytes {
		return size, nil
	}

	for len(m.state.Observations) > 0 && size > maxBytes {
		m.pruneOldestObservation("size")
		size, err = m.currentStateBytes()
		if err != nil {
			return 0, err
		}
	}
	for size > maxBytes {
		pruned := m.pruneFindingsByStatus("resolved", 1, "size")
		if pruned == 0 {
			break
		}
		m.dirty = true
		size, err = m.currentStateBytes()
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func (m *Monitor) pruneOldestObservation(reason string) {
	var oldestKey string
	var oldestTime time.Time
	for key, obs := range m.state.Observations {
		var lastSeen time.Time
		if obs != nil {
			lastSeen = obs.LastSeen
		}
		if oldestKey == "" || lastSeen.Before(oldestTime) {
			oldestKey = key
			oldestTime = lastSeen
		}
	}
	if oldestKey == "" {
		return
	}
	delete(m.state.Observations, oldestKey)
	statePrunes.WithLabelValues(m.cfg.Namespace, "observation", reason).Inc()
	m.dirty = true
}

func (m *Monitor) currentStateBytes() (int, error) {
	data, err := json.Marshal(m.state)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// updateStateMetrics refreshes the cheap count gauges every poll. It does NOT
// serialize the state: the state_bytes gauge is set in maybeSave from the size
// computed for the write, so it updates at write cadence instead of forcing a
// full json.Marshal of the entire state on every poll (including idle polls).
func (m *Monitor) updateStateMetrics() {
	stateFindings.WithLabelValues(m.cfg.Namespace).Set(float64(len(m.state.Findings)))
	stateObservations.WithLabelValues(m.cfg.Namespace).Set(float64(len(m.state.Observations)))
}

type stateObservationItem struct {
	key      string
	lastSeen time.Time
}

type stateFindingItem struct {
	id       string
	lastSeen time.Time
}

func (m *Monitor) publish(ctx context.Context, finding Finding) error {
	if err := m.publisher.PublishFinding(ctx, finding); err != nil {
		publishAttempts.WithLabelValues(m.cfg.Namespace, "error").Inc()
		log.Printf("ERROR: failed to publish finding %s: %v", finding.ID, err)
		return err
	}
	publishAttempts.WithLabelValues(m.cfg.Namespace, "ok").Inc()
	findingsTotal.WithLabelValues(m.cfg.Namespace, finding.Classification, finding.Check).Inc()
	log.Printf("published finding id=%s namespace=%s kind=%s name=%s check=%s classification=%s score=%d ai_triage_required=%t",
		finding.ID, finding.Namespace, finding.Kind, finding.Name, finding.Check, finding.Classification, finding.Score, finding.AITriageRequired)
	return nil
}

func (m *Monitor) maybeSave(ctx context.Context, now time.Time) {
	if !m.dirty || (!m.lastWrite.IsZero() && now.Sub(m.lastWrite) < m.cfg.StateWriteInterval) {
		return
	}
	size, err := m.pruneForStateSize(m.cfg.MaxStateBytes)
	if err != nil {
		stateWrites.WithLabelValues(m.cfg.Namespace, "error").Inc()
		log.Printf("ERROR: failed to measure state before write: %v", err)
		return
	}
	if m.cfg.MaxStateBytes > 0 && size > m.cfg.MaxStateBytes {
		stateWrites.WithLabelValues(m.cfg.Namespace, "too-large").Inc()
		log.Printf("ERROR: state ConfigMap payload is too large after pruning: bytes=%d max_bytes=%d findings=%d observations=%d",
			size, m.cfg.MaxStateBytes, len(m.state.Findings), len(m.state.Observations))
		return
	}
	stateBytes.WithLabelValues(m.cfg.Namespace).Set(float64(size))
	if err := m.store.Save(ctx, m.state); err != nil {
		stateWrites.WithLabelValues(m.cfg.Namespace, "error").Inc()
		log.Printf("ERROR: failed to write state ConfigMap: %v", err)
		return
	}
	stateWrites.WithLabelValues(m.cfg.Namespace, "ok").Inc()
	m.lastWrite = now
	m.dirty = false
}

// loadSuppressions reads the SUPPRESS_CONFIGMAP (if configured) and builds a
// lookup set of "kind/name/reason" keys. Any field may be "*" to wildcard-match.
func (m *Monitor) loadSuppressions(ctx context.Context) {
	if m.cfg.SuppressConfigMapName == "" {
		m.suppressions = nil
		return
	}
	cm, err := m.kube.CoreV1().ConfigMaps(m.cfg.Namespace).Get(ctx, m.cfg.SuppressConfigMapName, metav1.GetOptions{})
	if err != nil {
		// suppression ConfigMap is optional — absence is not an error
		m.suppressions = nil
		return
	}
	s := make(map[string]struct{}, len(cm.Data))
	for key := range cm.Data {
		s[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	m.suppressions = s
}

func (m *Monitor) isSuppressed(f Finding) bool {
	if len(m.suppressions) == 0 {
		return false
	}
	kind := strings.ToLower(f.Kind)
	name := strings.ToLower(f.Name)
	reason := strings.ToLower(f.Reason)
	candidates := []string{
		fmt.Sprintf("%s/%s/%s", kind, name, reason),
		fmt.Sprintf("%s/%s/*", kind, name),
		fmt.Sprintf("%s/*/%s", kind, reason),
		fmt.Sprintf("*/%s/%s", name, reason),
		fmt.Sprintf("%s/*/*", kind),
		fmt.Sprintf("*/*/%s", reason),
		"*/*/*",
	}
	for _, c := range candidates {
		if _, ok := m.suppressions[c]; ok {
			return true
		}
	}
	return false
}

func (m *Monitor) updateActiveMetrics(active map[string]Finding) {
	counts := map[string]int{}
	for _, finding := range active {
		counts[finding.Classification]++
	}
	for _, classification := range []string{"watch", "suspicious", "degraded", "incident-likely"} {
		activeFindings.WithLabelValues(m.cfg.Namespace, classification).Set(float64(counts[classification]))
	}
}
