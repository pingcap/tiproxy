// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type mockKafkaProducer struct {
	messages []kafka.Message
}

func (m *mockKafkaProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaProducer) Close() error {
	return nil
}

func TestKafkaSinkWrite(t *testing.T) {
	producer := &mockKafkaProducer{}
	sink := &kafkaSink{writer: producer}
	rec := Record{
		SQL:    "select ?",
		DB:     "db1",
		Cost:   "1.000",
		ExTime: "20250906 17:03:50.222",
	}
	require.NoError(t, sink.Write(rec))

	require.Len(t, producer.messages, 1)
	var got Record
	require.NoError(t, json.Unmarshal(producer.messages[0].Value, &got))
	require.Equal(t, rec, got)
}

func TestNewKafkaSinkValidation(t *testing.T) {
	_, err := newKafkaSink(KafkaConfig{Topic: "t"})
	require.Error(t, err)

	_, err = newKafkaSink(KafkaConfig{Brokers: "127.0.0.1:9092", Topic: "t"})
	require.NoError(t, err)
}
