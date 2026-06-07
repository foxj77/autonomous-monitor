package main

import (
	"context"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

func TestNewFindingShape(t *testing.T) {
	f := NewFinding("ns1", "Pod", "p1", "pod-health", "not-ready", 65, []string{"e1"})

	if f.Source != "autonomous-monitor" {
		t.Fatalf("source = %q, want autonomous-monitor", f.Source)
	}
	if f.PayloadType != "namespace_finding" {
		t.Fatalf("payload_type = %q, want namespace_finding", f.PayloadType)
	}
	if f.Namespace != "ns1" || f.Kind != "Pod" || f.Name != "p1" {
		t.Fatalf("finding identifiers wrong: %+v", f)
	}
	if f.Classification != "degraded" {
		t.Fatalf("score 65 should map to degraded, got %q", f.Classification)
	}
	if f.Severity != "high" {
		t.Fatalf("score 65 should map to high severity, got %q", f.Severity)
	}
	if f.Status != "ongoing" {
		t.Fatalf("new findings should be ongoing, got %q", f.Status)
	}
	if f.ID == "" || findingID("ns1", "Pod", "p1", "pod-health", "not-ready") != f.ID {
		t.Fatalf("finding ID should be deterministic, got %q", f.ID)
	}
	if !f.FirstSeen.IsZero() {
		t.Fatalf("FirstSeen should be zero on a fresh finding, got %v", f.FirstSeen)
	}
}

func TestClassificationForScore(t *testing.T) {
	cases := []struct {
		score int
		want  string
	}{
		{-5, "healthy"},
		{0, "healthy"},
		{19, "healthy"},
		{20, "watch"},
		{39, "watch"},
		{40, "suspicious"},
		{59, "suspicious"},
		{60, "degraded"},
		{79, "degraded"},
		{80, "incident-likely"},
		{100, "incident-likely"},
		{250, "incident-likely"},
	}
	for _, tc := range cases {
		if got := classificationForScore(tc.score); got != tc.want {
			t.Errorf("classificationForScore(%d) = %q, want %q", tc.score, got, tc.want)
		}
	}
}

func TestSeverityForScore(t *testing.T) {
	cases := []struct {
		score int
		want  string
	}{
		{0, "low"},
		{39, "low"},
		{40, "medium"},
		{59, "medium"},
		{60, "high"},
		{79, "high"},
		{80, "critical"},
		{100, "critical"},
	}
	for _, tc := range cases {
		if got := severityForScore(tc.score); got != tc.want {
			t.Errorf("severityForScore(%d) = %q, want %q", tc.score, got, tc.want)
		}
	}
}

func TestClampScore(t *testing.T) {
	if clampScore(-1) != 0 {
		t.Errorf("clampScore(-1) should be 0")
	}
	if clampScore(101) != 100 {
		t.Errorf("clampScore(101) should be 100")
	}
	if clampScore(42) != 42 {
		t.Errorf("clampScore(42) should be 42")
	}
}

func TestScoreWaitingReason(t *testing.T) {
	cases := map[string]int{
		"CrashLoopBackOff":           85,
		"ImagePullBackOff":           75,
		"ErrImagePull":               75,
		"CreateContainerConfigError": 75,
		"CreateContainerError":       75,
		"RunContainerError":          75,
		"ContainerCreating":          0,
		"PodInitializing":            0,
		"":                           50,
		"Pending":                    50,
		"SomethingElse":              50,
	}
	for reason, want := range cases {
		if got := scoreWaitingReason(reason); got != want {
			t.Errorf("scoreWaitingReason(%q) = %d, want %d", reason, got, want)
		}
	}
}

func TestScoreEventReason(t *testing.T) {
	cases := map[string]int{
		"FailedScheduling":     75,
		"FailedMount":          75,
		"FailedAttachVolume":   75,
		"BackOff":              70,
		"CrashLoopBackOff":     70,
		"Unhealthy":            70,
		"OOMKilled":            70,
		"OOMKilling":           70,
		"Failed":               65,
		"FailedSync":           65,
		"ReconciliationFailed": 65,
		"SomethingElse":        50,
	}
	for reason, want := range cases {
		if got := scoreEventReason(reason); got != want {
			t.Errorf("scoreEventReason(%q) = %d, want %d", reason, got, want)
		}
	}
}

func TestNormalizeReason(t *testing.T) {
	cases := map[string]string{
		"":                "unknown",
		"   ":             "unknown",
		"Simple":          "simple",
		"CamelCase":       "camel-case",
		"With Space":      "with--space",
		"With_Underscore": "with--underscore",
		"With/Slash":      "with--slash",
		"AlreadyLower":    "already-lower",
		"Mixed_AB/x":      "mixed--a-b-x",
		"StartUpper":      "start-upper",
		"endsWithLower":   "ends-with-lower",
	}
	for in, want := range cases {
		if got := normalizeReason(in); got != want {
			t.Errorf("normalizeReason(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPodReady(t *testing.T) {
	ready := corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}}}
	if !podReady(ready) {
		t.Error("Pod with Ready=True should be ready")
	}
	notReady := corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}}}
	if podReady(notReady) {
		t.Error("Pod with Ready=False should not be ready")
	}
	unknown := corev1.Pod{}
	if podReady(unknown) {
		t.Error("Pod with no Ready condition should not be ready")
	}
}

func TestEventLastSeen(t *testing.T) {
	last := metav1.NewTime(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	eventTime := metav1.NewMicroTime(time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC))
	created := metav1.NewTime(time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC))

	if got := eventLastSeen(corev1.Event{LastTimestamp: last}); !got.Equal(last.Time) {
		t.Errorf("LastTimestamp should win, got %v", got)
	}
	if got := eventLastSeen(corev1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: created}, EventTime: eventTime}); !got.Equal(eventTime.Time) {
		t.Errorf("EventTime should win over CreationTimestamp, got %v", got)
	}
	if got := eventLastSeen(corev1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: created}}); !got.Equal(created.Time) {
		t.Errorf("CreationTimestamp should be the fallback, got %v", got)
	}
}

func TestAppendMemSample(t *testing.T) {
	out := appendMemSample(nil, 10, 5)
	if len(out) != 1 || out[0] != 10 {
		t.Fatalf("first append: %v", out)
	}
	out = appendMemSample(out, 20, 5)
	out = appendMemSample(out, 30, 5)
	out = appendMemSample(out, 40, 5)
	out = appendMemSample(out, 50, 5)
	if len(out) != 5 {
		t.Fatalf("expected 5 samples, got %d", len(out))
	}
	out = appendMemSample(out, 60, 5)
	if len(out) != 5 || out[len(out)-1] != 60 {
		t.Fatalf("expected rolling window of 5, got %v", out)
	}
	if out[0] != 20 {
		t.Fatalf("oldest sample should be evicted, got first=%d", out[0])
	}
}

func TestMemoryTrending(t *testing.T) {
	if trending, _ := memoryTrending(nil); trending {
		t.Error("nil samples should not trend")
	}
	if trending, _ := memoryTrending([]int64{50, 60}); trending {
		t.Error("fewer than 3 samples should not trend")
	}
	if trending, _ := memoryTrending([]int64{50, 55, 56}); trending {
		t.Error("delta <10 should not trend")
	}
	if trending, _ := memoryTrending([]int64{50, 50, 50}); trending {
		t.Error("flat samples should not trend")
	}
	if trending, _ := memoryTrending([]int64{60, 55, 50}); trending {
		t.Error("declining samples should not trend")
	}
	if trending, _ := memoryTrending([]int64{50, 55, 60}); !trending {
		t.Error("delta=10 should trend (boundary is >=10)")
	}
	if trending, _ := memoryTrending([]int64{50, 60, 61}); !trending {
		t.Error("delta=11 should trend")
	}
	trending, note := memoryTrending([]int64{50, 60, 75})
	if !trending || !strings.Contains(note, "50% to 75%") {
		t.Errorf("expected note to include deltas, got %q", note)
	}
}

func TestFmtMiB(t *testing.T) {
	if got := fmtMiB(512 * 1024); got != "512Ki" {
		t.Errorf("fmtMiB(512Ki) = %q, want 512Ki", got)
	}
	if got := fmtMiB(128 * 1024 * 1024); got != "128Mi" {
		t.Errorf("fmtMiB(128Mi) = %q, want 128Mi", got)
	}
}

func TestIsBuiltinGroup(t *testing.T) {
	builtins := []string{"apps", "batch", "rbac.authorization.k8s.io", "metrics.k8s.io"}
	for _, g := range builtins {
		if !isBuiltinGroup(g) {
			t.Errorf("isBuiltinGroup(%q) should be true", g)
		}
	}
	notBuiltins := []string{"cert-manager.io", "source.toolkit.fluxcd.io", "kagent.dev", "helm.toolkit.fluxcd.io"}
	for _, g := range notBuiltins {
		if isBuiltinGroup(g) {
			t.Errorf("isBuiltinGroup(%q) should be false", g)
		}
	}
}

func TestContains(t *testing.T) {
	if !contains([]string{"get", "list", "watch"}, "list") {
		t.Error("contains should find list")
	}
	if contains([]string{"get", "watch"}, "list") {
		t.Error("contains should not find list")
	}
	if contains(nil, "list") {
		t.Error("contains on nil should be false")
	}
}

func TestShouldScanLogs(t *testing.T) {
	notReady := corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}}}
	if !shouldScanLogs(notReady, 3) {
		t.Error("not-ready pod should trigger log scan")
	}
	crashing := corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
		{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
	}}}
	if !shouldScanLogs(crashing, 3) {
		t.Error("container in CrashLoopBackOff should trigger log scan")
	}
	healthy := corev1.Pod{Status: corev1.PodStatus{
		Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		ContainerStatuses: []corev1.ContainerStatus{{RestartCount: 1}},
	}}
	if shouldScanLogs(healthy, 3) {
		t.Error("healthy pod with low restarts should not trigger log scan")
	}
	restarting := corev1.Pod{Status: corev1.PodStatus{
		Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		ContainerStatuses: []corev1.ContainerStatus{{RestartCount: 5}},
	}}
	if !shouldScanLogs(restarting, 3) {
		t.Error("pod above restart threshold should trigger log scan")
	}
}

func TestDeploymentFindings(t *testing.T) {
	t.Run("available replicas below desired", func(t *testing.T) {
		replicas := int32(3)
		d := newDeployment("web", &replicas, 1)
		d.Status.ObservedGeneration = d.Generation
		findings := deploymentFindings("ns", d)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding, got %d", len(findings))
		}
		if findings[0].Reason != "available-replicas-below-desired" {
			t.Errorf("reason = %q", findings[0].Reason)
		}
	})
	t.Run("generation lag", func(t *testing.T) {
		replicas := int32(2)
		d := newDeployment("web", &replicas, 2)
		d.Generation = 5
		d.Status.ObservedGeneration = 3
		findings := deploymentFindings("ns", d)
		var found bool
		for _, f := range findings {
			if f.Reason == "generation-lag" {
				found = true
			}
		}
		if !found {
			t.Errorf("expected generation-lag finding, got %+v", findings)
		}
	})
	t.Run("healthy deployment emits no findings", func(t *testing.T) {
		replicas := int32(2)
		d := newDeployment("web", &replicas, 2)
		d.Status.ObservedGeneration = d.Generation
		findings := deploymentFindings("ns", d)
		if len(findings) != 0 {
			t.Errorf("expected no findings, got %+v", findings)
		}
	})
}

func TestCheckPodsReportsCrashLoopBackOff(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-0",
			Namespace:         "ns1",
			CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				},
			},
		},
	}
	client := fake.NewSimpleClientset(pod)
	monitor := newTestMonitor(client, nil, nil, "ns1")

	result := monitor.checkPods(ctx, monitor.buildSnapshot(ctx, now))
	if !result.complete {
		t.Fatal("check should be complete")
	}
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	if result.findings[0].Reason != "crash-loop-back-off" {
		t.Errorf("reason = %q", result.findings[0].Reason)
	}
	if result.findings[0].Score != 85 {
		t.Errorf("score = %d, want 85", result.findings[0].Score)
	}
}

func TestCheckPodsReportsOOMKilled(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-0",
			Namespace:         "ns1",
			CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{},
					},
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"},
					},
				},
			},
		},
	}
	client := fake.NewSimpleClientset(pod)
	monitor := newTestMonitor(client, nil, nil, "ns1")

	result := monitor.checkPods(ctx, monitor.buildSnapshot(ctx, now))
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	if result.findings[0].Reason != "oom-killed" {
		t.Errorf("reason = %q", result.findings[0].Reason)
	}
}

func TestCheckResourceSpecsReportsMissingRequests(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns1"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "no-mem", Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
				}},
				{Name: "no-cpu", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
				}},
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewSimpleClientset(pod)
	monitor := newTestMonitor(client, nil, nil, "ns1")

	result := monitor.checkResourceSpecs(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	reasons := map[string]bool{}
	for _, f := range result.findings {
		reasons[f.Reason] = true
	}
	if !reasons["missing-memory-request"] {
		t.Errorf("expected missing-memory-request, got %v", reasons)
	}
	if !reasons["missing-memory-limit"] {
		t.Errorf("expected missing-memory-limit, got %v", reasons)
	}
	if !reasons["missing-cpu-request"] {
		t.Errorf("expected missing-cpu-request, got %v", reasons)
	}
}

func TestCheckEventsEmitsWarningFinding(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "e1", Namespace: "ns1"},
		Type:       corev1.EventTypeWarning,
		Reason:     "BackOff",
		Message:    "Back-off restarting failed container",
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "p1",
			Namespace: "ns1",
		},
		LastTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
	}
	client := fake.NewSimpleClientset(ev)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	monitor.cfg.EventLookback = 30 * time.Minute

	result := monitor.checkEvents(ctx, monitor.buildSnapshot(ctx, now))
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	f := result.findings[0]
	if f.Check != "event" {
		t.Errorf("check = %q", f.Check)
	}
	if !f.MatchingKubernetesEventFound {
		t.Error("MatchingKubernetesEventFound should be true")
	}
	if f.Reason != "back-off" {
		t.Errorf("reason = %q, want back-off", f.Reason)
	}
}

func TestCheckEventsSkipsEventsOutsideLookback(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "e1", Namespace: "ns1"},
		Type:       corev1.EventTypeWarning,
		Reason:     "BackOff",
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Name: "p1", Namespace: "ns1",
		},
		LastTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
	}
	client := fake.NewSimpleClientset(ev)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	monitor.cfg.EventLookback = 30 * time.Minute

	result := monitor.checkEvents(ctx, monitor.buildSnapshot(ctx, now))
	if len(result.findings) != 0 {
		t.Errorf("old event should be ignored, got %d findings", len(result.findings))
	}
}

func TestCheckWorkloadsReportsStatefulSetBelowDesired(t *testing.T) {
	ctx := context.Background()
	replicas := int32(3)
	sts := newStatefulSet("db", &replicas, 1)
	client := fake.NewSimpleClientset(&sts)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	disableAllExcept(monitor, "workloads")

	result := monitor.checkWorkloads(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	if result.findings[0].Reason != "ready-replicas-below-desired" {
		t.Errorf("reason = %q", result.findings[0].Reason)
	}
}

func TestCheckServicesReportsSelectorWithNoPods(t *testing.T) {
	ctx := context.Background()
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ns1"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "api"},
			Ports:    []corev1.ServicePort{{Port: 80}},
		},
	}
	client := fake.NewSimpleClientset(service)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	disableAllExcept(monitor, "services")

	result := monitor.checkServices(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	if !result.complete {
		t.Fatal("check should be complete")
	}
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	if result.findings[0].Check != "service" || result.findings[0].Reason != "selector-matches-no-pods" {
		t.Fatalf("unexpected finding: %+v", result.findings[0])
	}
}

func TestCheckServicesSkipsSelectorWithMatchingPod(t *testing.T) {
	ctx := context.Background()
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ns1"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "api"},
			Ports:    []corev1.ServicePort{{Port: 80}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-0",
			Namespace: "ns1",
			Labels:    map[string]string{"app": "api"},
		},
	}
	client := fake.NewSimpleClientset(service, pod)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	disableAllExcept(monitor, "services")

	result := monitor.checkServices(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	if len(result.findings) != 0 {
		t.Fatalf("expected no findings, got %+v", result.findings)
	}
}

func TestCheckServicesReportsPendingLoadBalancer(t *testing.T) {
	ctx := context.Background()
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "public", Namespace: "ns1"},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{Port: 443}},
		},
	}
	client := fake.NewSimpleClientset(service)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	disableAllExcept(monitor, "services")

	result := monitor.checkServices(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	if result.findings[0].Reason != "load-balancer-pending" {
		t.Fatalf("reason = %q, want load-balancer-pending", result.findings[0].Reason)
	}
}

func TestCheckPVCsReportsPendingAndLostClaims(t *testing.T) {
	ctx := context.Background()
	pending := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns1"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	lost := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "lost", Namespace: "ns1"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimLost},
	}
	client := fake.NewSimpleClientset(pending, lost)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	disableAllExcept(monitor, "pvcs")

	result := monitor.checkPVCs(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	reasons := map[string]bool{}
	for _, finding := range result.findings {
		reasons[finding.Reason] = true
	}
	if !reasons["pending"] || !reasons["lost"] {
		t.Fatalf("expected pending and lost PVC findings, got %+v", result.findings)
	}
}

func TestCheckPVCsReportsTrueConditions(t *testing.T) {
	ctx := context.Background()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns1"},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
			Conditions: []corev1.PersistentVolumeClaimCondition{
				{Type: corev1.PersistentVolumeClaimFileSystemResizePending, Status: corev1.ConditionTrue, Message: "waiting for pod restart"},
			},
		},
	}
	client := fake.NewSimpleClientset(pvc)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	disableAllExcept(monitor, "pvcs")

	result := monitor.checkPVCs(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	if result.findings[0].Reason != "condition-file-system-resize-pending-true" {
		t.Fatalf("reason = %q, want condition-file-system-resize-pending-true", result.findings[0].Reason)
	}
}

func TestCheckScalingReportsHPAConditionsAndReplicaPressure(t *testing.T) {
	ctx := context.Background()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns1"},
		Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 5},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{
			CurrentReplicas: 5,
			DesiredReplicas: 5,
			Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{
				{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionFalse, Reason: "FailedGetResourceMetric"},
				{Type: autoscalingv2.ScalingLimited, Status: corev1.ConditionTrue, Reason: "TooManyReplicas"},
			},
		},
	}
	client := fake.NewSimpleClientset(hpa)
	monitor := newTestMonitor(client, nil, nil, "ns1")
	disableAllExcept(monitor, "scaling")

	result := monitor.checkScaling(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	reasons := map[string]bool{}
	for _, finding := range result.findings {
		reasons[finding.Reason] = true
	}
	for _, want := range []string{"scaling-active-false", "scaling-limited", "max-replicas-reached"} {
		if !reasons[want] {
			t.Fatalf("expected %q finding, got %+v", want, result.findings)
		}
	}
}

func TestCheckCustomResourcesReportsGenerationLag(t *testing.T) {
	ctx := context.Background()

	gvr := schema.GroupVersionResource{Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "helmreleases"}
	gvrList := metav1.APIResource{Name: "helmreleases", Kind: "HelmRelease", Namespaced: true, Verbs: metav1.Verbs{"get", "list"}}

	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{gvr: "HelmReleaseList"}
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       "HelmRelease",
			"metadata": map[string]interface{}{
				"name":       "my-app",
				"namespace":  "ns1",
				"generation": int64(5),
			},
			"status": map[string]interface{}{
				"observedGeneration": int64(3),
				"conditions": []interface{}{
					map[string]interface{}{"type": "Ready", "status": "True"},
				},
			},
		},
	})

	client := fake.NewSimpleClientset()
	client.Resources = []*metav1.APIResourceList{
		{GroupVersion: "source.toolkit.fluxcd.io/v1", APIResources: []metav1.APIResource{gvrList}},
	}

	monitor := newTestMonitor(client, nil, dyn, "ns1")
	disableAllExcept(monitor, "custom-resources")

	apiLists, discoveryErr := monitor.customResourceAPILists()
	result := monitor.checkCustomResources(ctx, apiLists, discoveryErr)
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(result.findings))
	}
	if result.findings[0].Reason != "generation-lag" {
		t.Errorf("reason = %q", result.findings[0].Reason)
	}
}

func TestCheckCustomResourcesReportsFalseCondition(t *testing.T) {
	ctx := context.Background()

	gvr := schema.GroupVersionResource{Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "helmreleases"}
	gvrList := metav1.APIResource{Name: "helmreleases", Kind: "HelmRelease", Namespaced: true, Verbs: metav1.Verbs{"get", "list"}}

	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{gvr: "HelmReleaseList"}
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       "HelmRelease",
			"metadata": map[string]interface{}{
				"name":       "broken",
				"namespace":  "ns1",
				"generation": int64(5),
			},
			"status": map[string]interface{}{
				"observedGeneration": int64(5),
				"conditions": []interface{}{
					map[string]interface{}{"type": "Ready", "status": "False", "message": "install failed"},
				},
			},
		},
	})

	client := fake.NewSimpleClientset()
	client.Resources = []*metav1.APIResourceList{
		{GroupVersion: "source.toolkit.fluxcd.io/v1", APIResources: []metav1.APIResource{gvrList}},
	}

	monitor := newTestMonitor(client, nil, dyn, "ns1")
	disableAllExcept(monitor, "custom-resources")

	apiLists, discoveryErr := monitor.customResourceAPILists()
	result := monitor.checkCustomResources(ctx, apiLists, discoveryErr)
	if len(result.findings) != 1 {
		t.Fatalf("expected 1 finding, got %d: %+v", len(result.findings), result.findings)
	}
	if result.findings[0].Reason != "condition-ready-false" {
		t.Errorf("reason = %q", result.findings[0].Reason)
	}
}

func TestCustomResourceAllowlistAndExcludelist(t *testing.T) {
	monitor := newTestMonitor(fake.NewSimpleClientset(), nil, nil, "ns1")
	source := schema.GroupVersionResource{Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "helmrepositories"}
	helm := schema.GroupVersionResource{Group: "helm.toolkit.fluxcd.io", Version: "v2", Resource: "helmreleases"}
	noisy := schema.GroupVersionResource{Group: "noisy.example.com", Version: "v1", Resource: "widgets"}

	monitor.cfg.CustomResourceAllowlist = []string{"source.toolkit.fluxcd.io", "helm.toolkit.fluxcd.io/v2/helmreleases", "noisy.example.com/*"}
	monitor.cfg.CustomResourceExcludelist = []string{"noisy.example.com/widgets"}

	if !monitor.customResourceAllowed(source) {
		t.Fatal("group allowlist should allow source resources")
	}
	if !monitor.customResourceAllowed(helm) {
		t.Fatal("group/version/resource allowlist should allow helmreleases")
	}
	if monitor.customResourceAllowed(noisy) {
		t.Fatal("excludelist should override allowlist")
	}
}

func TestCustomResourceDiscoveryIsCached(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.Resources = []*metav1.APIResourceList{{
		GroupVersion: "source.toolkit.fluxcd.io/v1",
		APIResources: []metav1.APIResource{{
			Name:       "helmrepositories",
			Kind:       "HelmRepository",
			Namespaced: true,
			Verbs:      metav1.Verbs{"list"},
		}},
	}}
	monitor := newTestMonitor(client, nil, nil, "ns1")
	monitor.cfg.CustomResourceDiscoveryTTL = time.Hour

	first, err := monitor.customResourceAPILists()
	if err != nil {
		t.Fatalf("first discovery: %v", err)
	}
	client.Resources = nil
	second, err := monitor.customResourceAPILists()
	if err != nil {
		t.Fatalf("cached discovery: %v", err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("cached discovery lengths = first %d second %d, want both 1", len(first), len(second))
	}
}

func TestCheckResourceUsageIncrementsSampleMetric(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ns1"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("100Mi"),
			}},
		}}},
	})
	metricsClient := metricsfake.NewSimpleClientset()
	metricsClient.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() != "ns1" {
			t.Fatalf("metrics list namespace = %q, want ns1", action.GetNamespace())
		}
		return true, &metricsv1beta1.PodMetricsList{Items: []metricsv1beta1.PodMetrics{{
			ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ns1"},
			Containers: []metricsv1beta1.ContainerMetrics{{
				Name: "app",
				Usage: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("50Mi"),
				},
			}},
		}}}, nil
	})
	monitor := newTestMonitor(client, metricsClient, nil, "ns1")
	monitor.cfg.ResourceUsageBackend = "metrics-server"

	counter := resourceSamples.WithLabelValues("ns1", "metrics-server")
	before := counterValue(t, counter)
	result := monitor.checkResourceUsage(ctx, monitor.buildSnapshot(ctx, time.Now().UTC()))
	after := counterValue(t, counter)

	if !result.complete {
		t.Fatal("resource usage check should complete")
	}
	if after-before != 1 {
		t.Fatalf("resource sample counter delta = %v, want 1", after-before)
	}
}

func counterValue(t *testing.T, counter interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func newTestMonitor(client *fake.Clientset, metricsClient *metricsfake.Clientset, dyn *dynfake.FakeDynamicClient, namespace string) *Monitor {
	cfg := testConfig(namespace)
	store := NewStateStore(client, namespace, "autonomous-monitor-state")
	state := newMonitorState(namespace)
	return NewMonitor(cfg, client, metricsClient, dyn, &recordingPublisher{}, store, state)
}

func disableAllExcept(m *Monitor, active string) {
	m.cfg.Checks.Pods = active == "pods"
	m.cfg.Checks.Events = active == "events"
	m.cfg.Checks.Logs = active == "logs"
	m.cfg.Checks.Workloads = active == "workloads"
	m.cfg.Checks.ResourceUsage = active == "resource-usage"
	m.cfg.Checks.ResourceSpecs = active == "resource-specs"
	m.cfg.Checks.CustomResource = active == "custom-resources"
	m.cfg.Checks.Services = active == "services"
	m.cfg.Checks.Scaling = active == "scaling"
	m.cfg.Checks.PVCs = active == "pvcs"
}

func newDeployment(name string, replicas *int32, available int32) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns1", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: replicas},
		Status: appsv1.DeploymentStatus{
			AvailableReplicas:  available,
			ObservedGeneration: 1,
		},
	}
}

func newStatefulSet(name string, replicas *int32, ready int32) appsv1.StatefulSet {
	return appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns1"},
		Spec:       appsv1.StatefulSetSpec{Replicas: replicas},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: ready,
		},
	}
}

func TestIsSuppressed(t *testing.T) {
	m := &Monitor{suppressions: map[string]struct{}{
		"pod/my-pod/not-ready": {},
		"pod/*/restart-rate":   {},
		"*/my-pod/image-pull":  {},
		"deployment/*/*":       {},
		"*/*/oom-killed":       {},
	}}
	cases := []struct {
		kind, name, reason string
		want               bool
	}{
		// exact
		{"Pod", "my-pod", "not-ready", true},
		// pod/*/restart-rate matches any pod with that reason
		{"Pod", "other", "restart-rate", true},
		// */my-pod/image-pull matches any kind with that name and reason
		{"Deployment", "my-pod", "image-pull", true},
		// deployment/*/* matches any deployment
		{"Deployment", "x", "y", true},
		// */*/oom-killed matches any kind/name with oom-killed
		{"Pod", "any-name", "oom-killed", true},
		// negative cases
		{"Pod", "other", "not-ready", false},
		{"Pod", "my-pod", "other-reason", false},
		{"StatefulSet", "x", "y", false},
	}
	for _, c := range cases {
		if got := m.isSuppressed(Finding{Kind: c.kind, Name: c.name, Reason: c.reason}); got != c.want {
			t.Errorf("isSuppressed(%s/%s/%s) = %v, want %v", c.kind, c.name, c.reason, got, c.want)
		}
	}
}

func TestIsSuppressedEmptyMap(t *testing.T) {
	m := &Monitor{}
	if m.isSuppressed(Finding{Kind: "Pod", Name: "p", Reason: "r"}) {
		t.Error("empty suppression map should never suppress")
	}
}
