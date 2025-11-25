// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package meter

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/metering_sdk/common"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

const (
	writeInterval = 60
	// The timeout can not be too long because the pod grace termination period is fixed.
	writeTimeout = 10 * time.Second
	category     = "proxy"

	crossAZKey         = "crossZone_bytes"
	publicEndpointKey  = "public_outBound_bytes"
	privateEndpointKey = "private_outBound_bytes"
)

type MeterData struct {
	publicRespBytes  int64
	privateRespBytes int64
	crossAZBytes     int64
}

type Meter struct {
	sync.Mutex
	data   map[string]MeterData
	uuid   string
	writer *meteringwriter.MeteringWriter
	lg     *zap.Logger
	wg     waitgroup.WaitGroup
	cancel context.CancelFunc
}

func NewMeter(cfg *config.Config, lg *zap.Logger) (*Meter, error) {
	if len(cfg.Metering.Type) == 0 || len(cfg.Metering.Bucket) == 0 {
		return nil, nil
	}
	s3Config := cfg.Metering.ToProviderConfig()
	provider, err := storage.NewObjectStorageProvider(s3Config)
	if err != nil {
		lg.Error("Failed to create storage provider", zap.Error(err))
		return nil, err
	}
	meteringConfig := mconfig.DefaultConfig().WithLogger(lg.Named("metering_sdk"))
	writer := meteringwriter.NewMeteringWriterFromConfig(provider, meteringConfig, &cfg.Metering)
	return &Meter{
		lg:     lg,
		data:   make(map[string]MeterData),
		writer: writer,
		uuid:   strings.ReplaceAll(uuid.New().String(), "-", "_"), // no dash in the S3 path
	}, nil
}

func (m *Meter) IncTraffic(clusterID string, respBytes, crossAZBytes int64, fromPublicEndpoint bool) {
	m.Lock()
	defer m.Unlock()
	orig := m.data[clusterID]
	if fromPublicEndpoint {
		orig.publicRespBytes += respBytes
	} else {
		orig.privateRespBytes += respBytes
	}
	orig.crossAZBytes += crossAZBytes
	m.data[clusterID] = orig
}

func (m *Meter) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	m.wg.RunWithRecover(func() {
		m.flushLoop(ctx)
	}, nil, m.lg)
}

func (m *Meter) flushLoop(ctx context.Context) {
	m.lg.Info("metering is started")
	// Control the writing timestamp accurately enough so that the previous round won't be overwritten by the next round.
	curTime := time.Now().Unix()
	nextTime := curTime/writeInterval*writeInterval + writeInterval
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(nextTime-curTime) * time.Second):
			m.flush(nextTime, writeTimeout)
			nextTime += writeInterval
			curTime = time.Now().Unix()
		}
	}
	// Try our best to flush the final data even after closing.
	m.flush(nextTime, writeTimeout)
}

func (m *Meter) flush(ts int64, timeout time.Duration) {
	var data map[string]MeterData
	m.Lock()
	data = m.data
	m.data = make(map[string]MeterData, len(data))
	m.Unlock()

	if len(data) == 0 {
		return
	}
	array := make([]map[string]any, 0, len(data))
	for clusterID, d := range data {
		array = append(array, map[string]any{
			"version":          "1",
			"cluster_id":       clusterID,
			"source_name":      category,
			crossAZKey:         &common.MeteringValue{Value: uint64(d.crossAZBytes), Unit: "bytes"},
			privateEndpointKey: &common.MeteringValue{Value: uint64(d.privateRespBytes), Unit: "bytes"},
			publicEndpointKey:  &common.MeteringValue{Value: uint64(d.publicRespBytes), Unit: "bytes"},
		})
	}

	meteringData := &common.MeteringData{
		SelfID:    m.uuid,
		Timestamp: ts,
		Category:  category,
		Data:      array,
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := m.writer.Write(flushCtx, meteringData); err != nil {
		metrics.ServerErrCounter.WithLabelValues("metering").Inc()
		m.lg.Error("Failed to write metering data", zap.Error(err))
	}
	m.lg.Debug("flushed metering data", zap.Int("clusters", len(data)))
}

func (m *Meter) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	var err error
	if m.writer != nil {
		err = m.writer.Close()
	}
	m.lg.Debug("meter closed")
	return err
}
