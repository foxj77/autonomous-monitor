package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const stateDataKey = "state.json"
const stateSaveConflictRetries = 3

type MonitorState struct {
	Version      int                      `json:"version"`
	Namespace    string                   `json:"namespace"`
	Findings     map[string]*FindingState `json:"findings"`
	Observations map[string]*Observation  `json:"observations"`
}

type FindingState struct {
	Kind                    string     `json:"kind"`
	Name                    string     `json:"name"`
	Check                   string     `json:"check"`
	Reason                  string     `json:"reason"`
	FirstSeen               time.Time  `json:"first_seen"`
	LastSeen                time.Time  `json:"last_seen"`
	LastPublished           *time.Time `json:"last_published,omitempty"`
	LastPublishFailed       *time.Time `json:"last_publish_failed,omitempty"`
	LastAIDispatchRequested *time.Time `json:"last_ai_dispatch_requested,omitempty"`
	CooldownUntil           *time.Time `json:"cooldown_until,omitempty"`
	Score                   int        `json:"score"`
	Classification          string     `json:"classification"`
	Status                  string     `json:"status"`
}

type Observation struct {
	RestartCount       int32     `json:"restart_count,omitempty"`
	LastSeen           time.Time `json:"last_seen"`
	MemorySamplesPct   []int64   `json:"memory_samples_pct,omitempty"` // last N memory-usage-as-%-of-limit readings
	LastResourceSample time.Time `json:"last_resource_sample,omitempty"`
}

type StateStore struct {
	client    kubernetes.Interface
	namespace string
	name      string
	writeable bool
	configMap *corev1.ConfigMap
}

func NewStateStore(client kubernetes.Interface, namespace, name string) *StateStore {
	return &StateStore{
		client:    client,
		namespace: namespace,
		name:      name,
		writeable: true,
	}
}

func (s *StateStore) Load(ctx context.Context) (*MonitorState, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		state := newMonitorState(s.namespace)
		created, createErr := s.create(ctx, state)
		if createErr != nil {
			return nil, createErr
		}
		s.configMap = created
		log.Printf("created state ConfigMap %s/%s", s.namespace, s.name)
		return state, nil
	}
	if err != nil {
		return nil, err
	}

	s.configMap = cm
	raw := cm.Data[stateDataKey]
	if raw == "" {
		log.Printf("adopting existing empty state ConfigMap %s/%s", s.namespace, s.name)
		return newMonitorState(s.namespace), nil
	}

	var state MonitorState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		s.writeable = false
		return newMonitorState(s.namespace), fmt.Errorf("state ConfigMap %s/%s contains incompatible JSON; refusing to overwrite it: %w", s.namespace, s.name, err)
	}
	if state.Version != 1 {
		s.writeable = false
		return newMonitorState(s.namespace), fmt.Errorf("state ConfigMap %s/%s has unsupported version %d; refusing to overwrite it", s.namespace, s.name, state.Version)
	}
	if state.Namespace == "" {
		state.Namespace = s.namespace
	}
	if state.Findings == nil {
		state.Findings = map[string]*FindingState{}
	}
	if state.Observations == nil {
		state.Observations = map[string]*Observation{}
	}
	log.Printf("adopted state ConfigMap %s/%s with %d findings", s.namespace, s.name, len(state.Findings))
	return &state, nil
}

func (s *StateStore) Save(ctx context.Context, state *MonitorState) error {
	if !s.writeable {
		return fmt.Errorf("state ConfigMap %s/%s is not writeable", s.namespace, s.name)
	}
	// Compact JSON: the state ConfigMap is machine-read only. Indenting wasted
	// ~20-30% on whitespace (more etcd bytes, closer to the 1 MiB object limit)
	// and CPU. It also kept the written payload larger than the compact size
	// measured by currentStateBytes/maybeSave, so the MAX_STATE_BYTES guard now
	// matches what is actually written.
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < stateSaveConflictRetries; attempt++ {
		if s.configMap == nil || attempt > 0 {
			cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			s.configMap = cm
		}

		cm := s.configMap.DeepCopy()
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[stateDataKey] = string(data)

		updated, err := s.client.CoreV1().ConfigMaps(s.namespace).Update(ctx, cm, metav1.UpdateOptions{})
		if err == nil {
			s.configMap = updated
			return nil
		}
		lastErr = err
		if !apierrors.IsConflict(err) {
			return err
		}
		s.configMap = nil
	}
	return fmt.Errorf("state ConfigMap %s/%s update conflicted after %d attempts: %w", s.namespace, s.name, stateSaveConflictRetries, lastErr)
}

func (s *StateStore) create(ctx context.Context, state *MonitorState) (*corev1.ConfigMap, error) {
	// Compact JSON to match Save (machine-read only; saves etcd bytes and CPU).
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	return s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.name,
			Namespace: s.namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "autonomous-monitor",
				"app.kubernetes.io/component": "state",
			},
		},
		Data: map[string]string{
			stateDataKey: string(data),
		},
	}, metav1.CreateOptions{})
}

func newMonitorState(namespace string) *MonitorState {
	return &MonitorState{
		Version:      1,
		Namespace:    namespace,
		Findings:     map[string]*FindingState{},
		Observations: map[string]*Observation{},
	}
}
