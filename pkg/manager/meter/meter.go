// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package meter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/metering_sdk/common"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/tiproxy/lib/config"
	"go.uber.org/zap"
)

const (
	writeInterval = 60
)

type MeterData struct {
	respBytes    int64
	crossAZBytes int64
}

type Meter struct {
	sync.Mutex
	data   map[string]MeterData
	uuid   string
	writer *meteringwriter.MeteringWriter
	lg     *zap.Logger
}

func NewMeter(cfg *config.Config, lg *zap.Logger) (*Meter, error) {
	if len(cfg.Metering.Bucket) == 0 || len(cfg.Metering.Prefix) == 0 {
		return nil, errors.New("bucket or prefix is empty")
	}

	s3Config := &storage.ProviderConfig{
		Type:   storage.ProviderTypeS3,
		Bucket: cfg.Metering.Bucket,
		Region: cfg.Metering.Region,
		Prefix: cfg.Metering.Prefix,
	}
	provider, err := storage.NewObjectStorageProvider(s3Config)
	if err != nil {
		lg.Error("Failed to create storage provider", zap.Error(err))
		return nil, err
	}
	meteringConfig := mconfig.DefaultConfig().WithDevelopmentLogger()
	writer := meteringwriter.NewMeteringWriter(provider, meteringConfig)
	return &Meter{
		lg:     lg,
		data:   make(map[string]MeterData),
		writer: writer,
		uuid:   strings.ReplaceAll(uuid.New().String(), "-", "_"), // no dash in the S3 path
	}, nil
}

func (m *Meter) IncTraffic(clusterID string, respBytes, crossAZBytes int64) {
	m.Lock()
	defer m.Unlock()
	orig := m.data[clusterID]
	orig.respBytes += respBytes
	orig.crossAZBytes += crossAZBytes
	m.data[clusterID] = orig
}

func (m *Meter) Start(ctx context.Context) {
	curTime := time.Now().Unix()
	nextTime := curTime/writeInterval*writeInterval + writeInterval
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(nextTime-curTime) * time.Second):
			m.flush(ctx, nextTime)
			nextTime += writeInterval
			curTime = time.Now().Unix()
		}
	}
}

func (m *Meter) flush(ctx context.Context, ts int64) {
	var data map[string]MeterData
	m.Lock()
	data = m.data
	m.data = make(map[string]MeterData, len(data))
	m.Unlock()

	array := make([]map[string]any, len(data))
	for clusterID, d := range data {
		array = append(array, map[string]any{
			"version":         "v1",
			"cluster_id":      clusterID,
			"source_name":     "tiproxy",
			"timestamp":       ts,
			"crossZone_bytes": &common.MeteringValue{Value: uint64(d.crossAZBytes), Unit: "bytes"},
			"outBound_bytes":  &common.MeteringValue{Value: uint64(d.respBytes), Unit: "bytes"},
		})
	}

	meteringData := &common.MeteringData{
		SelfID:   m.uuid,
		Category: "tiproxy",
		Data:     array,
	}
	if err := m.writer.Write(ctx, meteringData); err != nil {
		m.lg.Error("Failed to write metering data", zap.Error(err))
	}
	m.lg.Debug("flushed metering data", zap.Int("clusters", len(data)))
}

func (m *Meter) Close() {
	if m.writer != nil {
		m.writer.Close()
	}
}
