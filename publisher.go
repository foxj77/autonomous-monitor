// Package main — Kafka publisher interface.
//
// Two implementations live next to this file:
//
//   - publisher_confluent.go (default, //go:build !kafka_pure):
//     confluent-kafka-go/v2, requires CGO and librdkafka at build time.
//
//   - publisher_pure.go       (//go:build kafka_pure):
//     twmb/franz-go, pure Go, no CGO. Slightly fewer knobs but identical
//     delivery-confirmation semantics from the caller's perspective.
//
// main.go and the monitor both depend on the Publisher interface below
// and never reference either concrete client directly, so swapping the
// build tag does not change observable behaviour.
package main

import "context"

// Publisher is the contract every Kafka backend must satisfy. The monitor
// only ever calls PublishFinding and Close. New backends should preserve
// delivery-confirmation semantics: PublishFinding must block until the
// broker acknowledges the message or the context (plus the optional
// delivery timeout) expires.
type Publisher interface {
	PublishFinding(context.Context, Finding) error
	Close()
}
