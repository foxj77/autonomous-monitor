// Tests for the Kafka Publisher interface and the franz-go backend.
package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Compile-time check that KafkaPublisher satisfies Publisher.
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

// TestFindingMarshalRoundTrip is hermetic. It guards the Finding JSON
// contract by re-marshaling a synthetic finding and verifying the
// published payload matches the documented schema.
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

// TestKafkaPublisherRoundTrip round-trips a finding through the publisher
// against an in-process kfake broker.
func TestKafkaPublisherRoundTrip(t *testing.T) {
	cluster, err := kfake.NewCluster()
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	defer cluster.Close()

	addrs := strings.Split(cluster.ListenAddrs()[0], ",")
	topic := "ns.findings"

	pub, err := NewKafkaPublisher(addrs[0], topic, 5*time.Second)
	if err != nil {
		t.Fatalf("NewKafkaPublisher: %v", err)
	}
	t.Cleanup(pub.Close)

	// Pre-create the topic so ProduceSync does not depend on auto-create.
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(context.Background(), 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Build a finding and publish it.
	now := time.Now().UTC().Truncate(time.Second)
	f := NewFinding("ns1", "Pod", "web-0", "pod-health", "not-ready", 65, []string{"pod not ready for 5m"})
	f.FirstSeen = now
	f.LastSeen = now
	f.AITriageRequired = true

	if err := pub.PublishFinding(context.Background(), f); err != nil {
		t.Fatalf("PublishFinding: %v", err)
	}

	// Read it back with a fresh consumer.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consumer.Close()

	pollCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fetches := consumer.PollFetches(pollCtx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("poll errors: %v", errs)
	}
	var records int
	var got Finding
	fetches.EachRecord(func(r *kgo.Record) {
		if err := json.Unmarshal(r.Value, &got); err != nil {
			t.Errorf("unmarshal: %v", err)
		}
		records++
	})
	if records != 1 {
		t.Fatalf("records = %d, want 1", records)
	}
	if got.ID != f.ID {
		t.Errorf("ID round-trip mismatch: got %q, want %q", got.ID, f.ID)
	}
	if got.Score != 65 {
		t.Errorf("Score round-trip mismatch: got %d, want 65", got.Score)
	}
	if got.Classification != "degraded" {
		t.Errorf("Classification round-trip mismatch: got %q, want degraded", got.Classification)
	}
}

// TestKafkaPublisherReportsDeliveryTimeout confirms the PublishTimeout
// knob is honoured and does not silently mask a broker outage.
func TestKafkaPublisherReportsDeliveryTimeout(t *testing.T) {
	cluster, err := kfake.NewCluster()
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	defer cluster.Close()

	addrs := strings.Split(cluster.ListenAddrs()[0], ",")
	topic := "ns.findings.timeout"

	// Tight 1ms delivery timeout against a broker we never produce to.
	pub, err := NewKafkaPublisher(addrs[0], topic, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("NewKafkaPublisher: %v", err)
	}
	t.Cleanup(pub.Close)

	f := NewFinding("ns1", "Pod", "web-0", "pod-health", "not-ready", 60, []string{"pod not ready"})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := pub.PublishFinding(ctx, f); err == nil {
		t.Fatal("expected PublishFinding to error on tight delivery timeout, got nil")
	}
}
