// Standalone consumer for the autonomous-monitor quickstart. Reads
// Findings from Kafka and prints them as a single human-readable line
// per record. Uses the pure-Go franz-go client — no CGO required.
//
// Build:
//   CGO_ENABLED=0 go build -o consumer .
//
// Environment variables:
//   KAFKA_BROKER  (default: localhost:9092)
//   KAFKA_TOPIC   (default: k8s.namespace.findings)
//   KAFKA_GROUP   (default: quickstart-consumer)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Finding is a local copy of the schema the monitor publishes. We
// intentionally do not import the main package so this binary stays
// standalone.
type Finding struct {
	Source         string   `json:"source"`
	Namespace      string   `json:"namespace"`
	Kind           string   `json:"kind"`
	Name           string   `json:"name"`
	Severity       string   `json:"severity"`
	Classification string   `json:"classification"`
	Score          int      `json:"score"`
	Check          string   `json:"check"`
	Reason         string   `json:"reason"`
	Status         string   `json:"status"`
	Evidence       []string `json:"evidence"`
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	broker := env("KAFKA_BROKER", "localhost:9092")
	topic := env("KAFKA_TOPIC", "k8s.namespace.findings")
	group := env("KAFKA_GROUP", "quickstart-consumer")

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("consuming topic=%s group=%s broker=%s", topic, group, broker)

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("poll error topic=%s partition=%d: %v", e.Topic, e.Partition, e.Err)
			}
			if ctx.Err() != nil {
				return
			}
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var f Finding
			if err := json.Unmarshal(r.Value, &f); err != nil {
				log.Printf("unmarshal: %v", err)
				return
			}
			evidence := strings.Join(f.Evidence, "; ")
			if evidence == "" {
				evidence = "-"
			}
			fmt.Printf("[%s] ns=%s %s/%s check=%s reason=%s score=%d status=%s :: %s\n",
				f.Severity, f.Namespace, f.Kind, f.Name, f.Check, f.Reason, f.Score, f.Status, evidence)
		})
		if ctx.Err() != nil {
			return
		}
	}
}
