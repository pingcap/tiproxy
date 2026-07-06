// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

// Sink writes formatted exec info records.
type Sink interface {
	Write(Record) error
	Close() error
}

// NewSinks creates sinks from file output path and optional Kafka config.
// Returns nil when neither output is configured.
func NewSinks(lg *zap.Logger, outputPath string, kafka KafkaConfig) ([]Sink, error) {
	var sinks []Sink
	if len(outputPath) > 0 {
		fileSink, err := newFileSink(outputPath)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, fileSink)
	}
	if len(kafka.Topic) > 0 {
		if err := kafka.Validate(); err != nil {
			for _, s := range sinks {
				_ = s.Close()
			}
			return nil, err
		}
		kafkaSink, err := newKafkaSink(lg, kafka)
		if err != nil {
			for _, s := range sinks {
				_ = s.Close()
			}
			return nil, err
		}
		sinks = append(sinks, kafkaSink)
	}
	return sinks, nil
}

func parseBrokers(brokers string) ([]string, error) {
	var addrs []string
	for _, part := range strings.Split(brokers, ",") {
		if addr := strings.TrimSpace(part); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		return nil, errors.New("kafka brokers is required when kafka topic is set")
	}
	return addrs, nil
}
