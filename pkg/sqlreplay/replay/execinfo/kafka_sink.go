// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/segmentio/kafka-go"
)

const (
	// RequireOne waits only for the leader ack instead of all replicas (default RequireAll).
	kafkaRequiredAcks = int(kafka.RequireOne)
	// kafkaBatchSize is larger than the kafka-go default (100) to reduce produce round trips.
	kafkaBatchSize = 4096
	// kafkaBatchTimeout flushes a partial batch instead of waiting for it to fill.
	kafkaBatchTimeout = 50 * time.Millisecond
)

type kafkaProducer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type kafkaSink struct {
	writer kafkaProducer
}

func newKafkaSink(cfg KafkaConfig) (*kafkaSink, error) {
	if len(cfg.Topic) == 0 {
		return nil, errors.New("kafka topic is required")
	}
	brokers, err := parseBrokers(cfg.Brokers)
	if err != nil {
		return nil, err
	}
	dialer, err := buildKafkaDialer(cfg)
	if err != nil {
		return nil, err
	}
	return &kafkaSink{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:       brokers,
			Topic:         cfg.Topic,
			Dialer:        dialer,
			Balancer:      &kafka.LeastBytes{},
			RequiredAcks:  kafkaRequiredAcks,
			BatchSize:     kafkaBatchSize,
			BatchTimeout:  kafkaBatchTimeout,
		}),
	}, nil
}

func (s *kafkaSink) Write(rec Record) error {
	value, err := json.Marshal(rec)
	if err != nil {
		return errors.Wrapf(err, "marshal exec info record")
	}
	if err := s.writer.WriteMessages(context.Background(), kafka.Message{Value: value}); err != nil {
		return errors.Wrapf(err, "write exec info to kafka")
	}
	return nil
}

func (s *kafkaSink) Close() error {
	return s.writer.Close()
}
