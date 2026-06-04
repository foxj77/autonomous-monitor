package main

import (
	"context"
	"fmt"
	"log"
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
	suppressions                   map[string]struct{} // loaded from SUPPRESS_CONFIGMAP each poll
	dirty                          bool
	lastWrite                      time.Time
	customResourceDiscovery        []*metav1.APIResourceList
	customResourceDiscoveryExpires time.Time
}

func NewMonitor(cfg Config, kube kubernetes.Interface, metricsClient metricsv1beta1client.Interface, dynClient dynamic.Interface, publisher Publisher, store *StateStore, state *MonitorState) *Monitor {
	return &Monitor{
		cfg:       cfg,
		kube:      kube,
		metrics:   metricsClient,
		dynamic:   dynClient,
		publisher: publisher,
		store:     store,
		state:     state,
	}
}

func (m *Monitor) Poll(ctx context.Context) {
	m.loadSuppressions(ctx)
	start := time.Now()
	now := start.UTC()
	active := map[string]Finding{}
	complete := true

	if m.cfg.Checks.Pods {
		result := m.runCheck(ctx, "pods", func(checkCtx context.Context) checkResult {
			return m.checkPods(checkCtx, now)
		})
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.ResourceSpecs {
		result := m.runCheck(ctx, "resource-spec", m.checkResourceSpecs)
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.Events {
		result := m.runCheck(ctx, "events", func(checkCtx context.Context) checkResult {
			return m.checkEvents(checkCtx, now)
		})
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.Workloads {
		result := m.runCheck(ctx, "workloads", m.checkWorkloads)
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.Services {
		result := m.runCheck(ctx, "services", m.checkServices)
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.PVCs {
		result := m.runCheck(ctx, "pvcs", m.checkPVCs)
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.Scaling {
		result := m.runCheck(ctx, "scaling", m.checkScaling)
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.Logs {
		result := m.runCheck(ctx, "logs", m.checkLogs)
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.ResourceUsage {
		result := m.runCheck(ctx, "resource-usage", func(checkCtx context.Context) checkResult {
			return m.checkResourceUsage(checkCtx, now)
		})
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}
	if m.cfg.Checks.CustomResource {
		result := m.runCheck(ctx, "custom-resources", m.checkCustomResources)
		complete = complete && result.complete
		m.collectFindings(ctx, result.findings, now, active)
	}

	if complete {
		m.resolveMissingFindings(ctx, now, active)
	}

	m.expireResolvedFindings(now)
	m.updateActiveMetrics(active)
	m.maybeSave(ctx, now)
	pollDuration.WithLabelValues(m.cfg.Namespace).Observe(time.Since(start).Seconds())
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

		if m.cfg.AITriageEnabled && finding.Score >= m.cfg.AIMinScore {
			if state.CooldownUntil == nil || now.After(*state.CooldownUntil) {
				cooldown := m.cfg.AICooldown
				if finding.Classification == "incident-likely" && m.cfg.AICooldownIncident > 0 {
					cooldown = m.cfg.AICooldownIncident
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
