package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Publisher interface {
	PublishFinding(context.Context, Finding) error
	Close()
}

type KafkaPublisher struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaPublisher(broker, topic string) (*KafkaPublisher, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})
	if err != nil {
		return nil, err
	}

	p := &KafkaPublisher{producer: producer, topic: topic}
	go func() {
		for e := range producer.Events() {
			if msg, ok := e.(*kafka.Message); ok && msg.TopicPartition.Error != nil {
				log.Printf("ERROR: failed to deliver finding to %s: %v", p.topic, msg.TopicPartition.Error)
			}
		}
	}()
	return p, nil
}

func (p *KafkaPublisher) PublishFinding(_ context.Context, finding Finding) error {
	data, err := json.Marshal(finding)
	if err != nil {
		return err
	}

	if err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(finding.ID),
		Value:          data,
	}, nil); err != nil {
		return fmt.Errorf("publish finding %s: %w", finding.ID, err)
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
