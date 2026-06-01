package main

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type recordingPublisher struct {
	findings []Finding
}

func (p *recordingPublisher) PublishFinding(_ context.Context, finding Finding) error {
	p.findings = append(p.findings, finding)
	return nil
}

func (p *recordingPublisher) Close() {}

func TestMonitorPublishesNotReadyFindingWithAIDispatchAndCooldown(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(notReadyPod("cert-manager", "cert-manager-webhook"))
	store := NewStateStore(client, "cert-manager", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	publisher := &recordingPublisher{}
	monitor := NewMonitor(testConfig("cert-manager"), client, nil, nil, publisher, store, state)

	monitor.Poll(ctx)
	if len(publisher.findings) != 1 {
		t.Fatalf("published findings after first poll = %d, want 1", len(publisher.findings))
	}
	if !publisher.findings[0].AITriageRequired {
		t.Fatal("expected first degraded finding to request AI triage")
	}
	if publisher.findings[0].CooldownUntil == nil {
		t.Fatal("expected AI cooldown to be set")
	}

	monitor.Poll(ctx)
	if len(publisher.findings) != 1 {
		t.Fatalf("published findings after second poll = %d, want still 1 due cooldown/dedup", len(publisher.findings))
	}
}

func TestMonitorPublishesResolvedFinding(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(notReadyPod("cert-manager", "cert-manager-webhook"))
	store := NewStateStore(client, "cert-manager", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	publisher := &recordingPublisher{}
	monitor := NewMonitor(testConfig("cert-manager"), client, nil, nil, publisher, store, state)

	monitor.Poll(ctx)
	if err := client.CoreV1().Pods("cert-manager").Delete(ctx, "cert-manager-webhook", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete pod: %v", err)
	}
	monitor.Poll(ctx)

	if len(publisher.findings) != 2 {
		t.Fatalf("published findings = %d, want finding + resolved", len(publisher.findings))
	}
	if publisher.findings[1].Status != "resolved" {
		t.Fatalf("second finding status = %q, want resolved", publisher.findings[1].Status)
	}
}

func TestResourceSpecsRunsWhenPodHealthDisabled(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(runningPodWithoutResources("flux-system", "source-controller"))
	store := NewStateStore(client, "flux-system", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	cfg := testConfig("flux-system")
	cfg.Checks.Pods = false
	cfg.Checks.ResourceSpecs = true
	publisher := &recordingPublisher{}
	monitor := NewMonitor(cfg, client, nil, nil, publisher, store, state)

	monitor.Poll(ctx)

	if len(publisher.findings) == 0 {
		t.Fatal("expected resource-spec findings even when pod health checks are disabled")
	}
	for _, finding := range publisher.findings {
		if finding.Check == "pod-health" {
			t.Fatalf("unexpected pod-health finding when CHECK_PODS_ENABLED=false: %+v", finding)
		}
	}
}

func TestMonitorExpiresOldResolvedFindings(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := NewStateStore(client, "kube-system", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	state.Findings["old-resolved"] = &FindingState{
		Kind:           "Pod",
		Name:           "coredns",
		Check:          "pod-health",
		Reason:         "not-ready",
		FirstSeen:      old,
		LastSeen:       old,
		Score:          0,
		Classification: "healthy",
		Status:         "resolved",
	}
	cfg := testConfig("kube-system")
	cfg.ResolvedFindingRetention = time.Hour
	cfg.Checks = CheckConfig{}
	publisher := &recordingPublisher{}
	monitor := NewMonitor(cfg, client, nil, nil, publisher, store, state)

	monitor.Poll(ctx)

	if _, ok := monitor.state.Findings["old-resolved"]; ok {
		t.Fatal("expected old resolved finding to be expired from state")
	}
}

func testConfig(namespace string) Config {
	return Config{
		Namespace:                namespace,
		PollInterval:             time.Minute,
		StateConfigMapName:       "autonomous-monitor-state",
		StateWriteInterval:       time.Hour,
		ResolvedFindingRetention: 24 * time.Hour,
		AITriageEnabled:          true,
		AIMinScore:               60,
		AICooldown:               30 * time.Minute,
		RestartWarningCount:      3,
		RestartWindow:            10 * time.Minute,
		EventLookback:            30 * time.Minute,
		Checks: CheckConfig{
			Pods:          true,
			Events:        false,
			Workloads:     false,
			ResourceSpecs: false,
		},
	}
}

func notReadyPod(namespace, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "app", Ready: false, RestartCount: 0},
			},
		},
	}
}

func runningPodWithoutResources(namespace, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "manager",
					Image: "example/manager:latest",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "manager", Ready: true},
			},
		},
	}
}
