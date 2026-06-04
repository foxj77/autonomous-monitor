// Tests for the Kafka Publisher interface and the two backend implementations.
//
// The default (confluent-kafka-go) backend requires librdkafka and a real
// broker to round-trip a finding, so the only test we can run portably
// against it is a compile-time interface conformance check.
//
// The pure-Go (franz-go) backend has an in-process fake broker in
// `github.com/twmb/franz-go/pkg/kfake`, so we round-trip findings against
// it. That test is gated behind the `kafka_pure` build tag and runs as
// part of `go test -tags kafka_pure ./...` in CI.
package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// Compile-time check that both backends satisfy Publisher. The build-tag
// selection means only one of the two struct types is defined in any
// given build, so the test only references the type that exists.
var _ Publisher = (*KafkaPublisher)(nil)

// TestPublisherInterfaceContract is the smallest test that protects
// the Publisher contract from accidental breaking changes. It checks
// the method set of the recording fake used by the monitor tests so
// the contract is enforced on both sides.
func TestPublisherInterfaceContract(t *testing.T) {
	rec := &recordingPublisher{}
	iface := reflect.TypeOf((*Publisher)(nil)).Elem()
	got := reflect.TypeOf(rec)
	if !got.Implements(iface) {
		t.Fatalf("recordingPublisher does not implement Publisher: %v", got)
	}
}

// TestFindingMarshalRoundTrip is hermetic and runs under every build tag.
// It guards the Finding JSON contract by re-marshaling a synthetic finding
// and verifying the published payload matches the documented schema.
func TestFindingMarshalRoundTrip(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	cooldown := now.Add(30 * time.Minute)
	f := NewFinding("ns1", "Pod", "web-0", "pod-health", "not-ready", 65, []string{"pod not ready for 5m"})
	f.FirstSeen = now
	f.LastSeen = now
	f.MatchingKubernetesEventFound = true
	f.AITriageRequired = true
	f.CooldownUntil = &cooldown

	data, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal finding: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal finding: %v", err)
	}

	for _, key := range []string{
		"source", "payload_type", "id", "namespace", "kind", "name",
		"severity", "classification", "score", "check", "reason", "status",
		"first_seen", "last_seen", "evidence",
		"matching_kubernetes_event_found", "ai_triage_required", "cooldown_until",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("Finding JSON missing required key %q", key)
		}
	}

	if got["source"] != "autonomous-monitor" {
		t.Errorf("source = %v, want autonomous-monitor", got["source"])
	}
	if got["payload_type"] != "namespace_finding" {
		t.Errorf("payload_type = %v, want namespace_finding", got["payload_type"])
	}
	if got["classification"] != "degraded" {
		t.Errorf("classification for score 65 = %v, want degraded", got["classification"])
	}
	if got["severity"] != "high" {
		t.Errorf("severity for score 65 = %v, want high", got["severity"])
	}
	if got["id"] == "" || len(got["id"].(string)) != 64 {
		t.Errorf("id = %v, want 64-char sha256 hex string", got["id"])
	}
}
