//go:build kafka_pure

// Round-trip tests for the pure-Go Kafka publisher against an in-process
// kfake broker. Only compiled under the kafka_pure build tag.
package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaPublisherRoundTripPure(t *testing.T) {
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
