package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestStateStoreCreatesMissingConfigMap(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := NewStateStore(client, "cert-manager", "autonomous-monitor-state")

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if state.Namespace != "cert-manager" {
		t.Fatalf("namespace = %q, want cert-manager", state.Namespace)
	}

	cm, err := client.CoreV1().ConfigMaps("cert-manager").Get(context.Background(), "autonomous-monitor-state", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected ConfigMap to be created: %v", err)
	}
	if cm.Data[stateDataKey] == "" {
		t.Fatal("created ConfigMap missing state data")
	}
}

func TestStateStoreAdoptsExistingConfigMap(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "autonomous-monitor-state",
			Namespace: "flux-system",
		},
		Data: map[string]string{
			stateDataKey: `{"version":1,"namespace":"flux-system","findings":{"abc":{"kind":"Pod","name":"source-controller","check":"pod-health","reason":"not-ready","first_seen":"2026-05-05T10:00:00Z","last_seen":"2026-05-05T10:00:00Z","score":60,"classification":"degraded","status":"ongoing"}},"observations":{}}`,
		},
	})
	store := NewStateStore(client, "flux-system", "autonomous-monitor-state")

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(state.Findings) != 1 {
		t.Fatalf("findings = %d, want 1", len(state.Findings))
	}
	if state.Findings["abc"].Name != "source-controller" {
		t.Fatalf("adopted finding name = %q", state.Findings["abc"].Name)
	}
}
