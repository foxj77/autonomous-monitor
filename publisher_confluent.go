//go:build !kafka_pure

// Default Kafka publisher, backed by confluent-kafka-go/v2. This is the
// production build target. It links against librdkafka and requires CGO
// to be enabled at build time. Use the kafka_pure build tag (see
// publisher_pure.go) to swap in the pure-Go franz-go client.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaPublisher publishes findings using a confluent-kafka-go Producer.
// deliveryTimeout bounds the per-publish wait for broker delivery
// confirmation; on expiry, PublishFinding returns ctx.Err()-wrapped.
type KafkaPublisher struct {
	producer        *kafka.Producer
	topic           string
	deliveryTimeout time.Duration
}

// NewKafkaPublisher constructs a KafkaPublisher for the given bootstrap
// broker and topic. The producer drains its event channel in a
// background goroutine so the librdkafka event queue never fills up.
func NewKafkaPublisher(broker, topic string, deliveryTimeout time.Duration) (*KafkaPublisher, error) {
	if deliveryTimeout <= 0 {
		deliveryTimeout = 10 * time.Second
	}
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})
	if err != nil {
		return nil, err
	}

	p := &KafkaPublisher{producer: producer, topic: topic, deliveryTimeout: deliveryTimeout}
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("ERROR: failed to deliver finding to %s: %v", p.topic, ev.TopicPartition.Error)
				}
			case kafka.Error:
				log.Printf("ERROR: kafka producer event for %s: %v", p.topic, ev)
			}
		}
	}()
	return p, nil
}

// PublishFinding marshals the finding, hands it to the producer, and
// waits for broker delivery confirmation bounded by deliveryTimeout
// (or the caller's context, whichever fires first).
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

	delivery := make(chan kafka.Event, 1)
	if err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(finding.ID),
		Value:          data,
	}, delivery); err != nil {
		return fmt.Errorf("publish finding %s: %w", finding.ID, err)
	}

	select {
	case e := <-delivery:
		msg, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("publish finding %s: unexpected kafka event %T", finding.ID, e)
		}
		if msg.TopicPartition.Error != nil {
			return fmt.Errorf("deliver finding %s to %s: %w", finding.ID, p.topic, msg.TopicPartition.Error)
		}
	case <-ctx.Done():
		return fmt.Errorf("deliver finding %s to %s: %w", finding.ID, p.topic, ctx.Err())
	}
	return nil
}

// Close flushes the producer queue and shuts down the underlying client.
// Safe to call on a nil receiver.
func (p *KafkaPublisher) Close() {
	if p == nil || p.producer == nil {
		return
	}
	p.producer.Flush(5000)
	p.producer.Close()
}
