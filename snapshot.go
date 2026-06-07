package main

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// snapshot holds the shared Kubernetes resources for a single poll. It is built
// exactly once on the main goroutine at the start of Poll, so each resource is
// listed once per poll instead of once per check. Checks read the snapshot and
// never touch m.state, which keeps abandoned (timed-out) check goroutines from
// racing the main goroutine's reads/writes of m.state.
//
// Each resource carries a success flag (e.g. podsOK). A check whose required
// inputs failed to list reports complete:false, preserving today's behavior
// where a List error skips resolveMissingFindings.
type snapshot struct {
	now time.Time

	pods     []corev1.Pod
	podsOK   bool
	events   []corev1.Event
	eventsOK bool

	deployments    []appsv1.Deployment
	deploymentsOK  bool
	statefulSets   []appsv1.StatefulSet
	statefulSetsOK bool
	daemonSets     []appsv1.DaemonSet
	daemonSetsOK   bool

	services   []corev1.Service
	servicesOK bool
	pvcs       []corev1.PersistentVolumeClaim
	pvcsOK     bool
	hpas       []autoscalingv2.HorizontalPodAutoscaler
	hpasOK     bool

	// priorObservations is a read-only value copy of the observations the pure
	// checks need (restart counts, memory samples). It is built on the main
	// goroutine so check goroutines never read m.state.Observations.
	priorObservations map[string]Observation
}

// buildSnapshot lists every shared resource once under a bounded context
// (reusing CheckTimeout) and copies the prior observations checks depend on.
// It records no checksTotal metrics: each check records its own ok/error
// counter from the relevant success flag, preserving existing metric labels.
func (m *Monitor) buildSnapshot(ctx context.Context, now time.Time) *snapshot {
	snap := &snapshot{now: now}

	// Read-only value copy of observations the pure checks need. This runs on
	// the main goroutine because it reads m.state; Observation is a value type
	// with a slice field, so the slice is copied to share no backing array.
	snap.priorObservations = make(map[string]Observation, len(m.state.Observations))
	for key, obs := range m.state.Observations {
		if obs == nil {
			continue
		}
		copied := *obs
		if obs.MemorySamplesPct != nil {
			copied.MemorySamplesPct = append([]int64(nil), obs.MemorySamplesPct...)
		}
		snap.priorObservations[key] = copied
	}

	// The live listing is bounded by CheckTimeout and raced against it, exactly
	// like runCheck. listSnapshot writes only into snap (never m.state), so if
	// the listing goroutine is abandoned on timeout it cannot race the main
	// goroutine. On timeout the returned snapshot keeps all *OK flags false, so
	// every check reports complete:false (matching today's List-error behavior).
	if m.cfg.CheckTimeout <= 0 {
		m.listSnapshot(ctx, snap)
		return snap
	}

	listCtx, cancel := context.WithTimeout(ctx, m.cfg.CheckTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		m.listSnapshot(listCtx, snap)
		close(done)
	}()

	select {
	case <-done:
		return snap
	case <-listCtx.Done():
		// Abandon the listing goroutine; it only writes into a snapshot the
		// main goroutine no longer reads. Return a fresh snapshot with the
		// prior observations and all *OK flags unset.
		abandoned := &snapshot{now: now, priorObservations: snap.priorObservations}
		return abandoned
	}
}

// listSnapshot performs the live List calls and records results into snap. It
// must never touch m.state so that an abandoned (timed-out) invocation cannot
// race the main goroutine.
func (m *Monitor) listSnapshot(ctx context.Context, snap *snapshot) {
	if pods, err := m.kube.CoreV1().Pods(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.pods = pods.Items
		snap.podsOK = true
	}
	if events, err := m.kube.CoreV1().Events(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.events = events.Items
		snap.eventsOK = true
	}
	if deployments, err := m.kube.AppsV1().Deployments(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.deployments = deployments.Items
		snap.deploymentsOK = true
	}
	if statefulSets, err := m.kube.AppsV1().StatefulSets(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.statefulSets = statefulSets.Items
		snap.statefulSetsOK = true
	}
	if daemonSets, err := m.kube.AppsV1().DaemonSets(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.daemonSets = daemonSets.Items
		snap.daemonSetsOK = true
	}
	if services, err := m.kube.CoreV1().Services(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.services = services.Items
		snap.servicesOK = true
	}
	if pvcs, err := m.kube.CoreV1().PersistentVolumeClaims(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.pvcs = pvcs.Items
		snap.pvcsOK = true
	}
	if hpas, err := m.kube.AutoscalingV2().HorizontalPodAutoscalers(m.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		snap.hpas = hpas.Items
		snap.hpasOK = true
	}
}
