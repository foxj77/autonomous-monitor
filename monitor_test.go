package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type recordingPublisher struct {
	findings []Finding
	err      error
}

func (p *recordingPublisher) PublishFinding(_ context.Context, finding Finding) error {
	p.findings = append(p.findings, finding)
	return p.err
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

func TestMonitorRetriesFailedFindingPublish(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(notReadyPod("cert-manager", "cert-manager-webhook"))
	store := NewStateStore(client, "cert-manager", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	publisher := &recordingPublisher{err: errors.New("broker down")}
	monitor := NewMonitor(testConfig("cert-manager"), client, nil, nil, publisher, store, state)

	monitor.Poll(ctx)
	if len(publisher.findings) != 1 {
		t.Fatalf("publish attempts after first poll = %d, want 1", len(publisher.findings))
	}
	failedState := monitor.state.Findings[publisher.findings[0].ID]
	if failedState.LastPublished != nil {
		t.Fatal("failed publish should not set LastPublished")
	}
	if failedState.LastPublishFailed == nil {
		t.Fatal("failed publish should record LastPublishFailed")
	}
	if failedState.CooldownUntil != nil {
		t.Fatal("failed AI dispatch publish should not start cooldown")
	}

	publisher.err = nil
	monitor.Poll(ctx)
	if len(publisher.findings) != 2 {
		t.Fatalf("publish attempts after retry = %d, want 2", len(publisher.findings))
	}
	retriedState := monitor.state.Findings[publisher.findings[1].ID]
	if retriedState.LastPublished == nil {
		t.Fatal("successful retry should set LastPublished")
	}
	if retriedState.LastPublishFailed != nil {
		t.Fatal("successful retry should clear LastPublishFailed")
	}
	if retriedState.CooldownUntil == nil {
		t.Fatal("successful retry should start AI cooldown")
	}
	if !publisher.findings[1].AITriageRequired {
		t.Fatal("successful retry should still request AI triage")
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

func TestMonitorRetriesFailedResolvedPublish(t *testing.T) {
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

	publisher.err = errors.New("broker down")
	monitor.Poll(ctx)
	if len(publisher.findings) != 2 {
		t.Fatalf("publish attempts after failed resolution = %d, want 2", len(publisher.findings))
	}
	stateAfterFailure := monitor.state.Findings[publisher.findings[0].ID]
	if stateAfterFailure.Status != "ongoing" {
		t.Fatalf("failed resolved publish should keep status ongoing, got %q", stateAfterFailure.Status)
	}
	if stateAfterFailure.LastPublishFailed == nil {
		t.Fatal("failed resolved publish should record LastPublishFailed")
	}

	publisher.err = nil
	monitor.Poll(ctx)
	if len(publisher.findings) != 3 {
		t.Fatalf("publish attempts after resolved retry = %d, want 3", len(publisher.findings))
	}
	stateAfterRetry := monitor.state.Findings[publisher.findings[0].ID]
	if stateAfterRetry.Status != "resolved" {
		t.Fatalf("successful resolved retry should mark resolved, got %q", stateAfterRetry.Status)
	}
	if stateAfterRetry.LastPublishFailed != nil {
		t.Fatal("successful resolved retry should clear LastPublishFailed")
	}
	if publisher.findings[2].Status != "resolved" {
		t.Fatalf("third publish status = %q, want resolved", publisher.findings[2].Status)
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

func TestConfiguredChecksAreWiredIntoPoll(t *testing.T) {
	cases := []struct {
		name      string
		active    string
		objects   []runtime.Object
		wantCheck string
	}{
		{
			name:   "services",
			active: "services",
			objects: []runtime.Object{
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ns1"},
					Spec: corev1.ServiceSpec{
						Selector: map[string]string{"app": "api"},
						Ports:    []corev1.ServicePort{{Port: 80}},
					},
				},
			},
			wantCheck: "service",
		},
		{
			name:   "pvcs",
			active: "pvcs",
			objects: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns1"},
					Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
				},
			},
			wantCheck: "pvc",
		},
		{
			name:   "scaling",
			active: "scaling",
			objects: []runtime.Object{
				&autoscalingv2.HorizontalPodAutoscaler{
					ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns1"},
					Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 5},
					Status: autoscalingv2.HorizontalPodAutoscalerStatus{
						CurrentReplicas: 1,
						DesiredReplicas: 3,
					},
				},
			},
			wantCheck: "scaling",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(tc.objects...)
			store := NewStateStore(client, "ns1", "autonomous-monitor-state")
			state, err := store.Load(context.Background())
			if err != nil {
				t.Fatalf("Load state: %v", err)
			}
			cfg := testConfig("ns1")
			cfg.Checks = CheckConfig{}
			switch tc.active {
			case "services":
				cfg.Checks.Services = true
			case "pvcs":
				cfg.Checks.PVCs = true
			case "scaling":
				cfg.Checks.Scaling = true
			default:
				t.Fatalf("unhandled check family %q", tc.active)
			}
			publisher := &recordingPublisher{}
			monitor := NewMonitor(cfg, client, nil, nil, publisher, store, state)

			monitor.Poll(context.Background())

			for _, finding := range publisher.findings {
				if finding.Check == tc.wantCheck {
					return
				}
			}
			t.Fatalf("expected %q finding from wired poll check, got %+v", tc.wantCheck, publisher.findings)
		})
	}
}

func TestPollTimesOutSlowCheck(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	checkStarted := make(chan struct{})
	checkDone := make(chan struct{})
	client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		close(checkStarted)
		time.Sleep(200 * time.Millisecond)
		close(checkDone)
		return true, &corev1.PodList{}, nil
	})
	store := NewStateStore(client, "ns1", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	cfg := testConfig("ns1")
	cfg.Checks = CheckConfig{Pods: true}
	cfg.CheckTimeout = 10 * time.Millisecond
	publisher := &recordingPublisher{}
	monitor := NewMonitor(cfg, client, nil, nil, publisher, store, state)

	start := time.Now()
	monitor.Poll(ctx)
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Fatalf("Poll elapsed = %s, want bounded by check timeout", elapsed)
	}
	select {
	case <-checkStarted:
	default:
		t.Fatal("expected pod check to start")
	}
	if len(publisher.findings) != 0 {
		t.Fatalf("timeout check should not publish findings, got %+v", publisher.findings)
	}
	select {
	case <-checkDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stalled check goroutine to exit")
	}
}

// TestPollAbandonedSlowCheckDoesNotRaceState proves the poll-timeout data race
// is gone. The pod list blocks well past CheckTimeout, so the listing goroutine
// is abandoned and keeps running while the main goroutine proceeds to prune and
// json.Marshal m.state. Before the fix, an abandoned check goroutine wrote
// m.state.Observations concurrently with that marshal, a concurrent map access
// that the race detector flags. With the fix the goroutine only writes a
// discarded snapshot, so this test passes cleanly under -race and the
// pre-existing state is left intact.
func TestPollAbandonedSlowCheckDoesNotRaceState(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	released := make(chan struct{})
	client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		// Block past CheckTimeout so the listing goroutine is abandoned while
		// the main goroutine continues into prune/marshal of m.state.
		<-released
		return true, &corev1.PodList{Items: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Name: "late", Namespace: "ns1"},
		}}}, nil
	})

	store := NewStateStore(client, "ns1", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	now := time.Now().UTC()
	// Seed observations and findings so the post-check prune/marshal touches
	// the same maps an abandoned check goroutine would have written.
	state.Observations["pod/late/restarts"] = &Observation{RestartCount: 1, LastSeen: now}
	state.Observations["pod/seed/memory"] = &Observation{MemorySamplesPct: []int64{10, 20, 30}, LastSeen: now}
	state.Findings["seed"] = &FindingState{Kind: "Pod", Name: "seed", Check: "pod-health", Reason: "not-ready", Status: "ongoing", FirstSeen: now, LastSeen: now}

	cfg := testConfig("ns1")
	cfg.Checks = CheckConfig{Pods: true, ResourceSpecs: true}
	cfg.CheckTimeout = 10 * time.Millisecond
	cfg.StateWriteInterval = 0
	monitor := NewMonitor(cfg, client, nil, nil, &recordingPublisher{}, store, state)

	done := make(chan struct{})
	go func() {
		monitor.Poll(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll did not return within the timeout budget")
	}

	// Pre-existing state must be intact: the abandoned check contributed nothing.
	if obs, ok := monitor.state.Observations["pod/late/restarts"]; !ok || obs.RestartCount != 1 {
		t.Fatalf("seed restart observation corrupted: %+v", monitor.state.Observations["pod/late/restarts"])
	}
	if _, ok := monitor.state.Findings["seed"]; !ok {
		t.Fatal("seed finding should remain after a poll whose checks all timed out")
	}

	// Let the abandoned listing goroutine finish; it must not touch m.state.
	close(released)
	time.Sleep(20 * time.Millisecond)
	if _, err := monitor.currentStateBytes(); err != nil {
		t.Fatalf("state should still marshal cleanly: %v", err)
	}
}

// TestPollListsPodsOncePerPoll asserts the fetch-once-per-poll behavior: with
// pods, resource-spec, logs and resource-usage all enabled (each of which used
// to list pods independently), a single Poll issues exactly one pods List.
func TestPollListsPodsOncePerPoll(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "ns1"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	})
	var podListCount int32
	client.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		atomic.AddInt32(&podListCount, 1)
		return false, nil, nil // fall through to the tracker's default list
	})

	store := NewStateStore(client, "ns1", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	cfg := testConfig("ns1")
	cfg.Checks = CheckConfig{Pods: true, ResourceSpecs: true, Logs: true, ResourceUsage: false}
	monitor := NewMonitor(cfg, client, nil, nil, &recordingPublisher{}, store, state)

	monitor.Poll(ctx)

	if got := atomic.LoadInt32(&podListCount); got != 1 {
		t.Fatalf("pods List count = %d across one poll, want exactly 1", got)
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

func TestMonitorPrunesStaleObservations(t *testing.T) {
	now := time.Now().UTC()
	monitor := &Monitor{
		cfg: testConfig("ns1"),
		state: &MonitorState{
			Version:   1,
			Namespace: "ns1",
			Findings:  map[string]*FindingState{},
			Observations: map[string]*Observation{
				"old":    {LastSeen: now.Add(-25 * time.Hour), RestartCount: 1},
				"recent": {LastSeen: now.Add(-time.Hour), RestartCount: 2},
			},
		},
	}
	monitor.cfg.ObservationRetention = 24 * time.Hour

	monitor.pruneState(now)

	if _, ok := monitor.state.Observations["old"]; ok {
		t.Fatal("old observation should be pruned")
	}
	if _, ok := monitor.state.Observations["recent"]; !ok {
		t.Fatal("recent observation should remain")
	}
}

func TestMonitorPrunesObservationCapOldestFirst(t *testing.T) {
	now := time.Now().UTC()
	monitor := &Monitor{
		cfg: testConfig("ns1"),
		state: &MonitorState{
			Version:   1,
			Namespace: "ns1",
			Findings:  map[string]*FindingState{},
			Observations: map[string]*Observation{
				"oldest": {LastSeen: now.Add(-3 * time.Hour)},
				"middle": {LastSeen: now.Add(-2 * time.Hour)},
				"newest": {LastSeen: now.Add(-time.Hour)},
			},
		},
	}
	monitor.cfg.MaxObservations = 2

	monitor.pruneState(now)

	if _, ok := monitor.state.Observations["oldest"]; ok {
		t.Fatal("oldest observation should be pruned")
	}
	if len(monitor.state.Observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(monitor.state.Observations))
	}
}

func TestMonitorPrunesResolvedFindingsBeforeOngoing(t *testing.T) {
	now := time.Now().UTC()
	monitor := &Monitor{
		cfg: testConfig("ns1"),
		state: &MonitorState{
			Version:      1,
			Namespace:    "ns1",
			Observations: map[string]*Observation{},
			Findings: map[string]*FindingState{
				"old-resolved": {Status: "resolved", LastSeen: now.Add(-3 * time.Hour)},
				"new-resolved": {Status: "resolved", LastSeen: now.Add(-time.Hour)},
				"ongoing":      {Status: "ongoing", LastSeen: now.Add(-2 * time.Hour)},
			},
		},
	}
	monitor.cfg.MaxFindings = 2

	monitor.pruneState(now)

	if _, ok := monitor.state.Findings["old-resolved"]; ok {
		t.Fatal("oldest resolved finding should be pruned first")
	}
	if _, ok := monitor.state.Findings["ongoing"]; !ok {
		t.Fatal("ongoing finding should remain while resolved findings can be pruned")
	}
}

func TestMonitorRefusesOversizedStateAfterPruning(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "autonomous-monitor-state", Namespace: "ns1"},
		Data:       map[string]string{stateDataKey: `{"version":1,"namespace":"ns1","findings":{},"observations":{}}`},
	})
	store := NewStateStore(client, "ns1", "autonomous-monitor-state")
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	now := time.Now().UTC()
	state.Findings["ongoing"] = &FindingState{
		Kind:           "Pod",
		Name:           strings.Repeat("x", 200),
		Check:          "pod-health",
		Reason:         "not-ready",
		Status:         "ongoing",
		LastSeen:       now,
		FirstSeen:      now,
		Score:          60,
		Classification: "degraded",
	}
	cfg := testConfig("ns1")
	cfg.MaxStateBytes = 64
	monitor := NewMonitor(cfg, client, nil, nil, &recordingPublisher{}, store, state)
	monitor.dirty = true

	monitor.maybeSave(ctx, now)

	if !monitor.lastWrite.IsZero() {
		t.Fatal("oversized state should not be written")
	}
}

func testConfig(namespace string) Config {
	return Config{
		Namespace:                namespace,
		PollInterval:             time.Minute,
		StateConfigMapName:       "autonomous-monitor-state",
		StateWriteInterval:       time.Hour,
		ResolvedFindingRetention: 24 * time.Hour,
		ObservationRetention:     24 * time.Hour,
		MaxObservations:          5000,
		MaxFindings:              2000,
		MaxStateBytes:            900 * 1024,
		DownstreamTriageEnabled:  true,
		DownstreamMinScore:       60,
		DownstreamCooldown:       30 * time.Minute,
		RestartWarningCount:      3,
		RestartWindow:            10 * time.Minute,
		EventLookback:            30 * time.Minute,
		CheckTimeout:             30 * time.Second,
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
