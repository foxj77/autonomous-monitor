//go:build kafka_pure

// Pure-Go Kafka publisher, backed by twmb/franz-go. Build with:
//
//	go build -tags 'musl kafka_pure' -o autonomous-monitor .
//
// This is the contributor-friendly path: no CGO, no librdkafka, no
// Alpine musl-dev package, no `apt install librdkafka-dev` on every
// fresh laptop. The Finding JSON contract and the rest of the binary
// are identical to the default confluent-kafka-go build.
//
// Delivery confirmation is provided by ProduceSync, which blocks until
// the broker acknowledges the record or the context expires. The
// per-publish delivery timeout is applied the same way the default
// publisher applies it: a context.WithTimeout layered on top of the
// caller's context.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher publishes findings using a franz-go client. deliveryTimeout
// bounds the per-publish wait for broker delivery confirmation; on
// expiry, PublishFinding returns a context.DeadlineExceeded-wrapped
// error.
type KafkaPublisher struct {
	client          *kgo.Client
	topic           string
	deliveryTimeout time.Duration
}

// NewKafkaPublisher constructs a KafkaPublisher for the given bootstrap
// broker and topic. The client is configured with conservative
// defaults: idempotent producer, AllISRAcks, and a 5s record linger so
// small bursts coalesce into a single request without adding tail
// latency. Internal retries are capped at 10 (franz-go's default) which
// matches the implicit retry budget the confluent-kafka-go path had.
func NewKafkaPublisher(broker, topic string, deliveryTimeout time.Duration) (*KafkaPublisher, error) {
	if broker == "" {
		return nil, errors.New("kafka: empty broker address")
	}
	if topic == "" {
		return nil, errors.New("kafka: empty topic name")
	}
	if deliveryTimeout <= 0 {
		deliveryTimeout = 10 * time.Second
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(10),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	return &KafkaPublisher{
		client:          client,
		topic:           topic,
		deliveryTimeout: deliveryTimeout,
	}, nil
}

// PublishFinding marshals the finding and blocks on ProduceSync until
// the broker acknowledges the record. The caller's context is layered
// with the publisher's delivery timeout so either cancellation source
// is respected.
func (p *KafkaPublisher) PublishFinding(ctx context.Context, finding Finding) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p.deliveryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.deliveryTimeout)
		defer cancel()
	}

	data, err := json.Marshal(finding)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(finding.ID),
		Value: data,
	}

	res := p.client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("deliver finding %s to %s: %w", finding.ID, p.topic, err)
	}
	return nil
}

// Close flushes pending records and shuts down the underlying client.
// Safe to call on a nil receiver.
func (p *KafkaPublisher) Close() {
	if p == nil || p.client == nil {
		return
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.client.Flush(flushCtx)
	p.client.Close()
}
