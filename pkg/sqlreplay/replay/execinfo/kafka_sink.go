// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	// RequireOne waits only for the leader ack instead of all replicas (default RequireAll).
	kafkaRequiredAcks = int(kafka.RequireOne)
	// kafkaBatchSize is larger than the kafka-go default (100) to reduce produce round trips.
	kafkaBatchSize = 4096
	// kafkaBatchTimeout flushes a partial batch instead of waiting for it to fill.
	kafkaBatchTimeout = 50 * time.Millisecond

	kafkaWorkerCount = 4
	// kafkaRecordsChBuf decouples replay from kafka workers.
	kafkaRecordsChBuf = 1 << 16
	// kafkaSinkBatchSize is the number of messages each worker sends per WriteMessages call.
	kafkaSinkBatchSize = 256
	// kafkaSinkFlushTimeout flushes a partial worker batch.
	kafkaSinkFlushTimeout = 50 * time.Millisecond
)

type kafkaProducer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type kafkaSink struct {
	writer    kafkaProducer
	recordsCh chan kafka.Message
	wg        sync.WaitGroup
	lg        *zap.Logger
}

func newKafkaSink(lg *zap.Logger, cfg KafkaConfig) (*kafkaSink, error) {
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
	s := &kafkaSink{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Topic:        cfg.Topic,
			Dialer:       dialer,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafkaRequiredAcks,
			BatchSize:    kafkaBatchSize,
			BatchTimeout: kafkaBatchTimeout,
		}),
		recordsCh: make(chan kafka.Message, kafkaRecordsChBuf),
		lg:        lg,
	}
	s.startWorkers()
	return s, nil
}

func (s *kafkaSink) startWorkers() {
	for range kafkaWorkerCount {
		s.wg.Add(1)
		go s.worker()
	}
}

func (s *kafkaSink) Write(rec Record) error {
	value, err := json.Marshal(rec)
	if err != nil {
		return errors.Wrapf(err, "marshal exec info record")
	}
	s.recordsCh <- kafka.Message{Value: value}
	return nil
}

func (s *kafkaSink) worker() {
	defer s.wg.Done()

	batch := make([]kafka.Message, 0, kafkaSinkBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.writer.WriteMessages(context.Background(), batch...); err != nil && s.lg != nil {
			s.lg.Warn("write exec info batch to kafka failed",
				zap.Error(err),
				zap.Int("batch_size", len(batch)))
		}
		batch = batch[:0]
	}

	flushTimer := time.NewTimer(kafkaSinkFlushTimeout)
	defer flushTimer.Stop()

	for {
		select {
		case msg, ok := <-s.recordsCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, msg)
			if len(batch) >= kafkaSinkBatchSize {
				flush()
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flushTimer.Reset(kafkaSinkFlushTimeout)
			}
		case <-flushTimer.C:
			flush()
			flushTimer.Reset(kafkaSinkFlushTimeout)
		}
	}
}

func (s *kafkaSink) Close() error {
	close(s.recordsCh)
	s.wg.Wait()
	return s.writer.Close()
}
