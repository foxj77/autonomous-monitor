package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Publisher interface {
	PublishFinding(context.Context, Finding) error
	Close()
}

type KafkaPublisher struct {
	producer        *kafka.Producer
	topic           string
	deliveryTimeout time.Duration
}

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

func (p *KafkaPublisher) Close() {
	if p == nil || p.producer == nil {
		return
	}
	p.producer.Flush(5000)
	p.producer.Close()
}
