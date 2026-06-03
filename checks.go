package main

import (
	"bufio"
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// logPatterns are scanned against the last N lines of a container's log.
// minCount is the minimum number of matching lines required to emit a finding.
var logPatterns = []struct {
	re       *regexp.Regexp
	reason   string
	score    int
	minCount int
}{
	{regexp.MustCompile(`(?i)\bpanic\b`), "log-panic", 85, 1},
	{regexp.MustCompile(`(?i)\bfatal\b`), "log-fatal", 80, 1},
	{regexp.MustCompile(`(?i)oomkilled|out of memory`), "log-oom-risk", 75, 1},
	{regexp.MustCompile(`(?i)x509:|tls:.*handshake`), "log-tls-error", 65, 1},
	{regexp.MustCompile(`(?i)certificate.*(expired|not yet valid)`), "log-cert-expired", 70, 1},
	{regexp.MustCompile(`(?i)connection refused`), "log-connection-refused", 60, 3},
	{regexp.MustCompile(`(?i)context deadline exceeded|i/o timeout`), "log-timeout", 55, 3},
	{regexp.MustCompile(`(?i)\berror\b`), "log-error", 45, 10},
}

type checkResult struct {
	findings []Finding
	complete bool
}

func (m *Monitor) checkPods(ctx context.Context, now time.Time) checkResult {
	pods, err := m.kube.CoreV1().Pods(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "pods", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "pods", "ok").Inc()

	var findings []Finding
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		age := now.Sub(pod.CreationTimestamp.Time)
		podName := pod.Name

		if pod.Status.Phase == corev1.PodPending && age > 5*time.Minute {
			findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", podName, "pod-health", "pending-too-long", 55, []string{
				fmt.Sprintf("pod phase is Pending for %s", age.Round(time.Second)),
			}))
		}

		if !podReady(pod) && age > 5*time.Minute {
			findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", podName, "pod-health", "not-ready", 60, []string{
				fmt.Sprintf("pod has not been Ready for %s", age.Round(time.Second)),
			}))
		}

		var totalRestarts int32
		for _, status := range pod.Status.ContainerStatuses {
			totalRestarts += status.RestartCount

			if status.State.Waiting != nil {
				reason := status.State.Waiting.Reason
				score := scoreWaitingReason(reason)
				if score > 0 {
					evidence := []string{fmt.Sprintf("container %s is waiting: %s", status.Name, reason)}
					if status.State.Waiting.Message != "" {
						evidence = append(evidence, status.State.Waiting.Message)
					}
					findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", podName, "pod-health", normalizeReason(reason), score, evidence))
				}
			}

			if status.LastTerminationState.Terminated != nil && status.LastTerminationState.Terminated.Reason == "OOMKilled" {
				findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", podName, "pod-health", "oom-killed", 85, []string{
					fmt.Sprintf("container %s last terminated with OOMKilled", status.Name),
				}))
			}
		}

		obsKey := fmt.Sprintf("pod/%s/restarts", podName)
		if obs, ok := m.state.Observations[obsKey]; ok && now.Sub(obs.LastSeen) <= m.cfg.RestartWindow {
			if delta := totalRestarts - obs.RestartCount; delta >= m.cfg.RestartWarningCount {
				findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", podName, "restart-rate", "restarts-increased", 65, []string{
					fmt.Sprintf("restart count increased by %d within %s", delta, m.cfg.RestartWindow),
					fmt.Sprintf("previous restarts=%d current restarts=%d", obs.RestartCount, totalRestarts),
				}))
			}
		}
		m.state.Observations[obsKey] = &Observation{RestartCount: totalRestarts, LastSeen: now}

	}

	return checkResult{findings: findings, complete: true}
}

func (m *Monitor) checkResourceSpecs(ctx context.Context) checkResult {
	pods, err := m.kube.CoreV1().Pods(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "resource-spec", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "resource-spec", "ok").Inc()

	var findings []Finding
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		findings = append(findings, m.checkPodResourceSpecs(pod)...)
	}
	return checkResult{findings: findings, complete: true}
}

func (m *Monitor) checkPodResourceSpecs(pod corev1.Pod) []Finding {
	var findings []Finding
	for _, c := range pod.Spec.Containers {
		if c.Resources.Requests == nil || c.Resources.Requests.Memory().IsZero() {
			findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", pod.Name+"/"+c.Name, "resource-spec", "missing-memory-request", 45, []string{
				fmt.Sprintf("container %s has no memory request", c.Name),
			}))
		}
		if c.Resources.Limits == nil || c.Resources.Limits.Memory().IsZero() {
			findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", pod.Name+"/"+c.Name, "resource-spec", "missing-memory-limit", 45, []string{
				fmt.Sprintf("container %s has no memory limit", c.Name),
			}))
		}
		if c.Resources.Requests == nil || c.Resources.Requests.Cpu().IsZero() {
			findings = append(findings, NewFinding(m.cfg.Namespace, "Pod", pod.Name+"/"+c.Name, "resource-spec", "missing-cpu-request", 30, []string{
				fmt.Sprintf("container %s has no CPU request", c.Name),
			}))
		}
	}
	return findings
}

func (m *Monitor) checkEvents(ctx context.Context, now time.Time) checkResult {
	events, err := m.kube.CoreV1().Events(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "events", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "events", "ok").Inc()

	findings := make([]Finding, 0, len(events.Items))
	for _, event := range events.Items {
		if event.Type != corev1.EventTypeWarning {
			continue
		}
		lastSeen := eventLastSeen(event)
		if now.Sub(lastSeen) > m.cfg.EventLookback {
			continue
		}
		name := event.InvolvedObject.Name
		kind := event.InvolvedObject.Kind
		if kind == "" {
			kind = "Object"
		}
		score := scoreEventReason(event.Reason)
		evidence := []string{fmt.Sprintf("Warning event %s observed %s: %s", event.Reason, lastSeen.Format(time.RFC3339), event.Message)}
		f := NewFinding(m.cfg.Namespace, kind, name, "event", normalizeReason(event.Reason), score, evidence)
		f.MatchingKubernetesEventFound = true
		findings = append(findings, f)
	}
	return checkResult{findings: findings, complete: true}
}

func (m *Monitor) checkWorkloads(ctx context.Context) checkResult {
	var findings []Finding
	complete := true

	deployments, err := m.kube.AppsV1().Deployments(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "deployments", "error").Inc()
		complete = false
	} else {
		checksTotal.WithLabelValues(m.cfg.Namespace, "deployments", "ok").Inc()
		for _, deployment := range deployments.Items {
			findings = append(findings, deploymentFindings(m.cfg.Namespace, deployment)...)
		}
	}

	statefulSets, err := m.kube.AppsV1().StatefulSets(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "statefulsets", "error").Inc()
		complete = false
	} else {
		checksTotal.WithLabelValues(m.cfg.Namespace, "statefulsets", "ok").Inc()
		for _, sts := range statefulSets.Items {
			desired := int32(1)
			if sts.Spec.Replicas != nil {
				desired = *sts.Spec.Replicas
			}
			if desired > sts.Status.ReadyReplicas {
				findings = append(findings, NewFinding(m.cfg.Namespace, "StatefulSet", sts.Name, "workload", "ready-replicas-below-desired", 65, []string{
					fmt.Sprintf("desired replicas=%d ready replicas=%d", desired, sts.Status.ReadyReplicas),
				}))
			}
		}
	}

	daemonSets, err := m.kube.AppsV1().DaemonSets(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "daemonsets", "error").Inc()
		complete = false
	} else {
		checksTotal.WithLabelValues(m.cfg.Namespace, "daemonsets", "ok").Inc()
		for _, ds := range daemonSets.Items {
			if ds.Status.DesiredNumberScheduled > ds.Status.NumberReady {
				findings = append(findings, NewFinding(m.cfg.Namespace, "DaemonSet", ds.Name, "workload", "ready-pods-below-desired", 65, []string{
					fmt.Sprintf("desired scheduled=%d ready=%d", ds.Status.DesiredNumberScheduled, ds.Status.NumberReady),
				}))
			}
		}
	}

	return checkResult{findings: findings, complete: complete}
}

func deploymentFindings(namespace string, deployment appsv1.Deployment) []Finding {
	var findings []Finding
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	if desired > deployment.Status.AvailableReplicas {
		findings = append(findings, NewFinding(namespace, "Deployment", deployment.Name, "workload", "available-replicas-below-desired", 65, []string{
			fmt.Sprintf("desired replicas=%d available replicas=%d", desired, deployment.Status.AvailableReplicas),
		}))
	}
	if deployment.Status.ObservedGeneration < deployment.Generation {
		findings = append(findings, NewFinding(namespace, "Deployment", deployment.Name, "workload", "generation-lag", 50, []string{
			fmt.Sprintf("observed generation=%d metadata generation=%d", deployment.Status.ObservedGeneration, deployment.Generation),
		}))
	}
	return findings
}

func (m *Monitor) checkServices(ctx context.Context) checkResult {
	services, err := m.kube.CoreV1().Services(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "services", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "services", "ok").Inc()

	var findings []Finding
	for _, svc := range services.Items {
		if svc.DeletionTimestamp != nil || svc.Spec.Type == corev1.ServiceTypeExternalName {
			continue
		}

		if svc.Spec.Type == corev1.ServiceTypeLoadBalancer && len(svc.Status.LoadBalancer.Ingress) == 0 {
			findings = append(findings, NewFinding(m.cfg.Namespace, "Service", svc.Name, "service", "load-balancer-pending", 55, []string{
				"service type LoadBalancer has no ingress assigned",
			}))
		}

		if len(svc.Spec.Selector) == 0 {
			continue
		}
		selector := labels.SelectorFromSet(svc.Spec.Selector).String()
		pods, err := m.kube.CoreV1().Pods(m.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			checksTotal.WithLabelValues(m.cfg.Namespace, "services", "error").Inc()
			return checkResult{findings: findings, complete: false}
		}
		if countLivePods(pods.Items) == 0 {
			findings = append(findings, NewFinding(m.cfg.Namespace, "Service", svc.Name, "service", "selector-matches-no-pods", 60, []string{
				fmt.Sprintf("service selector %q matches no live pods", selector),
			}))
		}
	}
	return checkResult{findings: findings, complete: true}
}

func countLivePods(pods []corev1.Pod) int {
	count := 0
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			count++
		}
	}
	return count
}

func (m *Monitor) checkPVCs(ctx context.Context) checkResult {
	pvcs, err := m.kube.CoreV1().PersistentVolumeClaims(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "pvcs", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "pvcs", "ok").Inc()

	var findings []Finding
	for _, pvc := range pvcs.Items {
		if pvc.DeletionTimestamp != nil {
			continue
		}
		switch pvc.Status.Phase {
		case corev1.ClaimPending:
			findings = append(findings, NewFinding(m.cfg.Namespace, "PersistentVolumeClaim", pvc.Name, "pvc", "pending", 60, []string{
				"persistent volume claim is Pending",
			}))
		case corev1.ClaimLost:
			findings = append(findings, NewFinding(m.cfg.Namespace, "PersistentVolumeClaim", pvc.Name, "pvc", "lost", 85, []string{
				"persistent volume claim is Lost",
			}))
		}

		for _, condition := range pvc.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			reason := "condition-" + normalizeReason(string(condition.Type)) + "-true"
			evidence := []string{fmt.Sprintf("condition %s=True", condition.Type)}
			if condition.Message != "" {
				evidence = append(evidence, condition.Message)
			}
			findings = append(findings, NewFinding(m.cfg.Namespace, "PersistentVolumeClaim", pvc.Name, "pvc", reason, 55, evidence))
		}
	}
	return checkResult{findings: findings, complete: true}
}

func (m *Monitor) checkScaling(ctx context.Context) checkResult {
	hpas, err := m.kube.AutoscalingV2().HorizontalPodAutoscalers(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "scaling", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "scaling", "ok").Inc()

	var findings []Finding
	for _, hpa := range hpas.Items {
		if hpa.DeletionTimestamp != nil {
			continue
		}
		findings = append(findings, hpaFindings(m.cfg.Namespace, hpa)...)
	}
	return checkResult{findings: findings, complete: true}
}

func hpaFindings(namespace string, hpa autoscalingv2.HorizontalPodAutoscaler) []Finding {
	var findings []Finding
	for _, condition := range hpa.Status.Conditions {
		evidence := []string{fmt.Sprintf("condition %s=%s", condition.Type, condition.Status)}
		if condition.Reason != "" {
			evidence = append(evidence, fmt.Sprintf("reason=%s", condition.Reason))
		}
		if condition.Message != "" {
			evidence = append(evidence, condition.Message)
		}

		switch {
		case condition.Type == autoscalingv2.ScalingActive && condition.Status == corev1.ConditionFalse:
			findings = append(findings, NewFinding(namespace, "HorizontalPodAutoscaler", hpa.Name, "scaling", "scaling-active-false", 70, evidence))
		case condition.Type == autoscalingv2.AbleToScale && condition.Status == corev1.ConditionFalse:
			findings = append(findings, NewFinding(namespace, "HorizontalPodAutoscaler", hpa.Name, "scaling", "able-to-scale-false", 65, evidence))
		case condition.Type == autoscalingv2.ScalingLimited && condition.Status == corev1.ConditionTrue:
			findings = append(findings, NewFinding(namespace, "HorizontalPodAutoscaler", hpa.Name, "scaling", "scaling-limited", 55, evidence))
		}
	}

	if hpa.Status.CurrentReplicas >= hpa.Spec.MaxReplicas && hpa.Status.DesiredReplicas >= hpa.Spec.MaxReplicas {
		findings = append(findings, NewFinding(namespace, "HorizontalPodAutoscaler", hpa.Name, "scaling", "max-replicas-reached", 65, []string{
			fmt.Sprintf("current replicas=%d desired replicas=%d max replicas=%d", hpa.Status.CurrentReplicas, hpa.Status.DesiredReplicas, hpa.Spec.MaxReplicas),
		}))
	}
	if hpa.Status.DesiredReplicas > hpa.Status.CurrentReplicas {
		findings = append(findings, NewFinding(namespace, "HorizontalPodAutoscaler", hpa.Name, "scaling", "desired-replicas-above-current", 55, []string{
			fmt.Sprintf("desired replicas=%d current replicas=%d", hpa.Status.DesiredReplicas, hpa.Status.CurrentReplicas),
		}))
	}

	return findings
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func eventLastSeen(event corev1.Event) time.Time {
	switch {
	case !event.LastTimestamp.IsZero():
		return event.LastTimestamp.Time
	case !event.EventTime.IsZero():
		return event.EventTime.Time
	default:
		return event.CreationTimestamp.Time
	}
}

func scoreWaitingReason(reason string) int {
	switch reason {
	case "CrashLoopBackOff":
		return 85
	case "ImagePullBackOff", "ErrImagePull":
		return 75
	case "CreateContainerConfigError", "CreateContainerError":
		return 75
	case "RunContainerError":
		return 75
	case "ContainerCreating", "PodInitializing":
		return 0
	default:
		return 50
	}
}

func scoreEventReason(reason string) int {
	switch reason {
	case "FailedScheduling", "FailedMount", "FailedAttachVolume":
		return 75
	case "BackOff", "CrashLoopBackOff", "Unhealthy", "OOMKilled", "OOMKilling":
		return 70
	case "Failed", "FailedSync", "ReconciliationFailed":
		return 65
	default:
		return 50
	}
}

func (m *Monitor) checkLogs(ctx context.Context) checkResult {
	pods, err := m.kube.CoreV1().Pods(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "logs", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "logs", "ok").Inc()

	var findings []Finding
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if !shouldScanLogs(pod, m.cfg.RestartWarningCount) {
			continue
		}
		for _, container := range pod.Spec.Containers {
			findings = append(findings, m.scanContainerLogs(ctx, pod.Name, container.Name)...)
		}
	}
	return checkResult{findings: findings, complete: true}
}

func shouldScanLogs(pod corev1.Pod, restartThreshold int32) bool {
	if !podReady(pod) {
		return true
	}
	var totalRestarts int32
	for _, s := range pod.Status.ContainerStatuses {
		totalRestarts += s.RestartCount
		if s.State.Waiting != nil && scoreWaitingReason(s.State.Waiting.Reason) > 0 {
			return true
		}
	}
	return totalRestarts >= restartThreshold
}

func (m *Monitor) scanContainerLogs(ctx context.Context, podName, containerName string) []Finding {
	lines := int64(m.cfg.LogScanLines)
	req := m.kube.CoreV1().Pods(m.cfg.Namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
		TailLines: &lines,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		// pod may be initialising or already gone; skip silently
		return nil
	}
	defer func() { _ = stream.Close() }()

	type match struct {
		count int
		lines []string
	}
	counts := make([]match, len(logPatterns))
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		line := scanner.Text()
		for i, p := range logPatterns {
			if p.re.MatchString(line) {
				counts[i].count++
				if len(counts[i].lines) < 3 {
					trimmed := line
					if len(trimmed) > 200 {
						trimmed = trimmed[:200] + "…"
					}
					counts[i].lines = append(counts[i].lines, trimmed)
				}
			}
		}
	}

	findings := make([]Finding, 0, len(logPatterns))
	for i, p := range logPatterns {
		if counts[i].count < p.minCount {
			continue
		}
		evidence := []string{
			fmt.Sprintf("found %d matching lines (pattern: %s) in last %d log lines", counts[i].count, p.reason, m.cfg.LogScanLines),
		}
		evidence = append(evidence, counts[i].lines...)
		findings = append(findings, NewFinding(
			m.cfg.Namespace, "Pod", podName+"/"+containerName,
			"log-pattern", p.reason, p.score, evidence,
		))
	}
	return findings
}

func (m *Monitor) checkResourceUsage(ctx context.Context, now time.Time) checkResult {
	if m.metrics == nil {
		return checkResult{complete: true}
	}

	podMetricsList, err := m.metrics.MetricsV1beta1().PodMetricses(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "resource-usage", "error").Inc()
		return checkResult{complete: false}
	}
	checksTotal.WithLabelValues(m.cfg.Namespace, "resource-usage", "ok").Inc()

	pods, err := m.kube.CoreV1().Pods(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "resource-usage", "error").Inc()
		return checkResult{complete: false}
	}

	// Build pod→total-memory-limit map from pod specs.
	memLimit := map[string]int64{}
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		var total resource.Quantity
		for _, c := range pod.Spec.Containers {
			if lim := c.Resources.Limits.Memory(); lim != nil && !lim.IsZero() {
				total.Add(*lim)
			}
		}
		if v := total.Value(); v > 0 {
			memLimit[pod.Name] = v
		}
	}

	var findings []Finding
	for i := range podMetricsList.Items {
		pm := &podMetricsList.Items[i]
		limit, ok := memLimit[pm.Name]
		if !ok || limit == 0 {
			continue
		}

		var used resource.Quantity
		for _, c := range pm.Containers {
			if u := c.Usage.Memory(); u != nil {
				used.Add(*u)
			}
		}
		usedBytes := used.Value()
		pct := usedBytes * 100 / limit

		obsKey := fmt.Sprintf("pod/%s/memory", pm.Name)
		if _, exists := m.state.Observations[obsKey]; !exists {
			m.state.Observations[obsKey] = &Observation{LastSeen: now}
		}
		obs := m.state.Observations[obsKey]
		obs.MemorySamplesPct = appendMemSample(obs.MemorySamplesPct, pct, 5)
		obs.LastResourceSample = now
		m.dirty = true
		resourceSamples.WithLabelValues(m.cfg.Namespace, m.cfg.ResourceUsageBackend).Inc()

		trending, trendNote := memoryTrending(obs.MemorySamplesPct)
		evidence := []string{
			fmt.Sprintf("memory usage %d%% of limit (%s / %s)", pct, fmtMiB(usedBytes), fmtMiB(limit)),
		}
		if trendNote != "" {
			evidence = append(evidence, trendNote)
		}

		score := 0
		reason := ""
		prediction := ""

		switch {
		case pct >= int64(m.cfg.MemoryCriticalPercent):
			score = 80
			reason = "memory-critical"
			prediction = fmt.Sprintf("pod likely to OOMKill: memory at %d%% of limit", pct)
		case pct >= int64(m.cfg.MemoryWarningPercent) && trending:
			score = 72
			reason = "memory-trending-to-limit"
			prediction = fmt.Sprintf("memory trending toward limit, currently at %d%%", pct)
		case pct >= int64(m.cfg.MemoryWarningPercent):
			score = 62
			reason = "memory-high"
		case trending && pct >= 50:
			score = 45
			reason = "memory-trending"
			prediction = fmt.Sprintf("memory trending upward, currently at %d%% of limit", pct)
		}

		if score > 0 {
			f := NewFinding(m.cfg.Namespace, "Pod", pm.Name, "resource-usage", reason, score, evidence)
			f.Prediction = prediction
			findings = append(findings, f)
		}
	}

	return checkResult{findings: findings, complete: true}
}

func appendMemSample(samples []int64, val int64, maxLen int) []int64 {
	samples = append(samples, val)
	if len(samples) > maxLen {
		samples = samples[len(samples)-maxLen:]
	}
	return samples
}

func memoryTrending(samples []int64) (bool, string) {
	if len(samples) < 3 {
		return false, ""
	}
	first, last := samples[0], samples[len(samples)-1]
	if last > first && last-first >= 10 {
		return true, fmt.Sprintf("memory rose from %d%% to %d%% across last %d samples", first, last, len(samples))
	}
	return false, ""
}

func fmtMiB(bytes int64) string {
	if bytes < 1024*1024 {
		return fmt.Sprintf("%dKi", bytes/1024)
	}
	return fmt.Sprintf("%dMi", bytes/1024/1024)
}

// unhealthyConditions are status.conditions values that indicate a resource is not healthy.
// A True status on a negative condition (Stalled, Reconciling>threshold) or False on a
// positive condition (Ready, Available, Healthy) raises a finding.
var negativeConditions = map[string]bool{"Stalled": true, "Reconciling": true, "Failed": true}
var positiveConditions = map[string]bool{"Ready": true, "Available": true, "Healthy": true, "Synced": true}

func (m *Monitor) checkCustomResources(ctx context.Context) checkResult {
	if m.dynamic == nil {
		return checkResult{complete: true}
	}

	_, apiLists, err := m.kube.Discovery().ServerGroupsAndResources()
	if err != nil {
		checksTotal.WithLabelValues(m.cfg.Namespace, "custom-resources", "error").Inc()
		return checkResult{complete: false}
	}

	var findings []Finding
	for _, apiList := range apiLists {
		gv, err := schema.ParseGroupVersion(apiList.GroupVersion)
		if err != nil || gv.Group == "" || isBuiltinGroup(gv.Group) {
			continue
		}
		for _, res := range apiList.APIResources {
			if !res.Namespaced || !contains(res.Verbs, "list") {
				continue
			}
			gvr := schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: res.Name}
			list, err := m.dynamic.Resource(gvr).Namespace(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				continue // RBAC denial or resource unavailable — skip silently
			}
			checksTotal.WithLabelValues(m.cfg.Namespace, "custom-resources", "ok").Inc()

			for i := range list.Items {
				obj := &list.Items[i]
				kind := obj.GetKind()
				if kind == "" {
					kind = res.Kind
				}
				name := obj.GetName()

				// Check observedGeneration lag.
				gen := obj.GetGeneration()
				if status, ok := obj.Object["status"].(map[string]interface{}); ok {
					if obsGen, ok := status["observedGeneration"].(int64); ok && gen > 0 && obsGen < gen {
						findings = append(findings, NewFinding(m.cfg.Namespace, kind, name, "custom-resource", "generation-lag", 55, []string{
							fmt.Sprintf("observedGeneration=%d metadata.generation=%d", obsGen, gen),
						}))
					}

					// Check status.conditions.
					if conds, ok := status["conditions"].([]interface{}); ok {
						for _, c := range conds {
							cmap, ok := c.(map[string]interface{})
							if !ok {
								continue
							}
							condType, _ := cmap["type"].(string)
							condStatus, _ := cmap["status"].(string)
							message, _ := cmap["message"].(string)
							if condType == "" || condStatus == "" {
								continue
							}
							evidence := []string{fmt.Sprintf("condition %s=%s", condType, condStatus)}
							if message != "" {
								evidence = append(evidence, message)
							}
							if positiveConditions[condType] && condStatus == "False" {
								findings = append(findings, NewFinding(m.cfg.Namespace, kind, name, "custom-resource",
									"condition-"+strings.ToLower(condType)+"-false", 65, evidence))
							}
							if negativeConditions[condType] && condStatus == "True" && condType != "Reconciling" {
								findings = append(findings, NewFinding(m.cfg.Namespace, kind, name, "custom-resource",
									"condition-"+strings.ToLower(condType)+"-true", 70, evidence))
							}
						}
					}
				}
			}
		}
	}

	return checkResult{findings: findings, complete: true}
}

func isBuiltinGroup(group string) bool {
	builtins := []string{
		"apps", "batch", "extensions", "networking.k8s.io", "policy",
		"rbac.authorization.k8s.io", "storage.k8s.io", "admissionregistration.k8s.io",
		"apiextensions.k8s.io", "coordination.k8s.io", "events.k8s.io",
		"autoscaling", "certificates.k8s.io", "authentication.k8s.io",
		"authorization.k8s.io", "metrics.k8s.io", "node.k8s.io",
		"scheduling.k8s.io", "discovery.k8s.io", "flowcontrol.apiserver.k8s.io",
	}
	for _, b := range builtins {
		if group == b {
			return true
		}
	}
	return false
}

func contains(verbs []string, verb string) bool {
	for _, v := range verbs {
		if v == verb {
			return true
		}
	}
	return false
}

func normalizeReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "unknown"
	}
	out := make([]rune, 0, len(reason)*2) // worst case: every char becomes a "x-y" pair
	for i, r := range reason {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				out = append(out, '-')
			}
			r = r + ('a' - 'A')
		}
		if r == '_' || r == ' ' || r == '/' {
			r = '-'
		}
		out = append(out, r)
	}
	return strings.ToLower(string(out))
}
