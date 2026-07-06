// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockKafkaProducer struct {
	mu       sync.Mutex
	messages []kafka.Message
}

func (m *mockKafkaProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaProducer) Close() error {
	return nil
}

func (m *mockKafkaProducer) snapshot() []kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]kafka.Message(nil), m.messages...)
}

func newTestKafkaSink(producer kafkaProducer) *kafkaSink {
	s := &kafkaSink{
		writer:    producer,
		recordsCh: make(chan kafka.Message, kafkaRecordsChBuf),
		lg:        zap.NewNop(),
	}
	s.startWorkers()
	return s
}

func TestKafkaSinkWrite(t *testing.T) {
	producer := &mockKafkaProducer{}
	sink := newTestKafkaSink(producer)
	rec := Record{
		SQL:    "select ?",
		DB:     "db1",
		Cost:   1000,
		ExTime: "20250906 17:03:50.222",
	}
	require.NoError(t, sink.Write(rec))
	require.NoError(t, sink.Close())

	messages := producer.snapshot()
	require.Len(t, messages, 1)
	var got Record
	require.NoError(t, json.Unmarshal(messages[0].Value, &got))
	require.Equal(t, rec, got)
}

func TestKafkaSinkBatchWrite(t *testing.T) {
	producer := &mockKafkaProducer{}
	sink := newTestKafkaSink(producer)

	const total = kafkaSinkBatchSize + 10
	for i := range total {
		require.NoError(t, sink.Write(Record{
			SQL:    "select ?",
			DB:     "db1",
			Cost:   int64(i),
			ExTime: "20250906 17:03:50.222",
		}))
	}
	require.NoError(t, sink.Close())

	messages := producer.snapshot()
	require.Len(t, messages, total)
}

func TestNewKafkaSinkValidation(t *testing.T) {
	_, err := newKafkaSink(zap.NewNop(), KafkaConfig{Topic: "t"})
	require.Error(t, err)

	sink, err := newKafkaSink(zap.NewNop(), KafkaConfig{Brokers: "127.0.0.1:9092", Topic: "t"})
	require.NoError(t, err)
	require.NoError(t, sink.Close())
}
