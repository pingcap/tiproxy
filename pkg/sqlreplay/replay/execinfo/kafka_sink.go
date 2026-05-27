// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/segmentio/kafka-go"
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
			Brokers:  brokers,
			Topic:    cfg.Topic,
			Dialer:   dialer,
			Balancer: &kafka.LeastBytes{},
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
