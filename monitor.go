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
	cfg          Config
	kube         kubernetes.Interface
	metrics      metricsv1beta1client.Interface
	dynamic      dynamic.Interface
	publisher    Publisher
	store        *StateStore
	state        *MonitorState
	suppressions map[string]struct{} // loaded from SUPPRESS_CONFIGMAP each poll
	dirty        bool
	lastWrite    time.Time
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
		result := m.checkPods(ctx, now)
		complete = complete && result.complete
		m.collectFindings(result.findings, now, active)
	}
	if m.cfg.Checks.ResourceSpecs {
		result := m.checkResourceSpecs(ctx)
		complete = complete && result.complete
		m.collectFindings(result.findings, now, active)
	}
	if m.cfg.Checks.Events {
		result := m.checkEvents(ctx, now)
		complete = complete && result.complete
		m.collectFindings(result.findings, now, active)
	}
	if m.cfg.Checks.Workloads {
		result := m.checkWorkloads(ctx)
		complete = complete && result.complete
		m.collectFindings(result.findings, now, active)
	}
	if m.cfg.Checks.Logs {
		result := m.checkLogs(ctx)
		complete = complete && result.complete
		m.collectFindings(result.findings, now, active)
	}
	if m.cfg.Checks.ResourceUsage {
		result := m.checkResourceUsage(ctx, now)
		complete = complete && result.complete
		m.collectFindings(result.findings, now, active)
	}
	if m.cfg.Checks.CustomResource {
		result := m.checkCustomResources(ctx)
		complete = complete && result.complete
		m.collectFindings(result.findings, now, active)
	}

	if complete {
		m.resolveMissingFindings(ctx, now, active)
	}

	m.expireResolvedFindings(now)
	m.updateActiveMetrics(active)
	m.maybeSave(ctx, now)
	pollDuration.WithLabelValues(m.cfg.Namespace).Observe(time.Since(start).Seconds())
}

func (m *Monitor) collectFindings(findings []Finding, now time.Time, active map[string]Finding) {
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
		shouldPublish := !exists || state.Status == "resolved" || finding.Score > state.Score || finding.Classification != state.Classification

		if m.cfg.AITriageEnabled && finding.Score >= m.cfg.AIMinScore {
			if state.CooldownUntil == nil || now.After(*state.CooldownUntil) {
				cooldown := m.cfg.AICooldown
				if finding.Classification == "incident-likely" && m.cfg.AICooldownIncident > 0 {
					cooldown = m.cfg.AICooldownIncident
				}
				cooldownUntil := now.Add(cooldown)
				state.LastAIDispatchRequested = &now
				state.CooldownUntil = &cooldownUntil
				finding.AITriageRequired = true
				finding.CooldownUntil = &cooldownUntil
				aiDispatchRequests.WithLabelValues(m.cfg.Namespace, "requested").Inc()
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
			m.publish(context.Background(), finding)
			state.LastPublished = &now
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
		m.publish(ctx, resolved)
		state.Status = "resolved"
		state.LastSeen = now
		state.Score = 0
		state.Classification = "healthy"
		state.LastPublished = &now
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

func (m *Monitor) publish(ctx context.Context, finding Finding) {
	if err := m.publisher.PublishFinding(ctx, finding); err != nil {
		log.Printf("ERROR: failed to publish finding %s: %v", finding.ID, err)
		return
	}
	findingsTotal.WithLabelValues(m.cfg.Namespace, finding.Classification, finding.Check).Inc()
	log.Printf("published finding id=%s namespace=%s kind=%s name=%s check=%s classification=%s score=%d ai_triage_required=%t",
		finding.ID, finding.Namespace, finding.Kind, finding.Name, finding.Check, finding.Classification, finding.Score, finding.AITriageRequired)
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
