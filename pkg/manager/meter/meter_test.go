// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package meter

import (
	"context"
	"testing"
	"time"

	mconfig "github.com/pingcap/metering_sdk/config"
	meteringreader "github.com/pingcap/metering_sdk/reader/metering"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func TestNewMeter(t *testing.T) {
	tests := []struct {
		cfg   config.Config
		isNil bool
	}{
		{
			cfg:   config.Config{},
			isNil: true,
		},
		{
			cfg: config.Config{
				Metering: config.Metering{
					Bucket: "bucket",
				},
			},
			isNil: false,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	for _, test := range tests {
		m, err := NewMeter(&test.cfg, lg)
		require.NoError(t, err)
		if test.isNil {
			require.Nil(t, m)
		} else {
			require.NotNil(t, m)
			m.Close()
			require.NotEmpty(t, m.uuid)
			require.NotContains(t, m.uuid, "-")
		}
	}
}

func TestWrite(t *testing.T) {
	m, reader := createLocalMeter(t, t.TempDir())
	ts := time.Now().Unix() / 60 * 60
	m.IncTraffic("cluster-1", 100, 200)
	m.IncTraffic("cluster-2", 200, 300)
	m.flush(ts, time.Second)

	data := readMeteringData(t, reader, ts)
	require.True(t, len(data) > 0)
	resp, crossAZ := getValuesFromData(t, data, "cluster-1")
	require.Equal(t, int64(100), resp)
	require.Equal(t, int64(200), crossAZ)

	resp, crossAZ = getValuesFromData(t, data, "cluster-2")
	require.Equal(t, int64(200), resp)
	require.Equal(t, int64(300), crossAZ)
}

func TestLoop(t *testing.T) {
	m, reader := createLocalMeter(t, t.TempDir())
	// The SDK only allows writing with a timestamp that is a multiple of 60.
	m.Start(context.Background())

	startTime := time.Now().Unix()
	var wg waitgroup.WaitGroup
	for range 10 {
		wg.Run(func() {
			for range 100 {
				m.IncTraffic("cluster-1", 1, 2)
				m.IncTraffic("cluster-2", 1, 2)
				time.Sleep(time.Millisecond)
			}
		}, nil)
	}
	wg.Wait()
	require.NoError(t, m.Close())

	totalResp, totalCrossAZ := make(map[string]int64), make(map[string]int64)
	for ts := startTime / 60 * 60; ts <= startTime/60*60+60; ts += 60 {
		data := readMeteringData(t, reader, ts)
		if len(data) == 0 {
			continue
		}
		resp, crossAZ := getValuesFromData(t, data, "cluster-1")
		totalResp["cluster-1"] += resp
		totalCrossAZ["cluster-1"] += crossAZ
		resp, crossAZ = getValuesFromData(t, data, "cluster-2")
		totalResp["cluster-2"] += resp
		totalCrossAZ["cluster-2"] += crossAZ
	}

	require.Equal(t, int64(1000), totalResp["cluster-1"])
	require.Equal(t, int64(2000), totalCrossAZ["cluster-1"])
	require.Equal(t, int64(1000), totalResp["cluster-2"])
	require.Equal(t, int64(2000), totalCrossAZ["cluster-2"])
}

func createLocalMeter(t *testing.T, dir string) (*Meter, *meteringreader.MeteringReader) {
	lg, _ := logger.CreateLoggerForTest(t)
	m, err := NewMeter(&config.Config{
		Metering: config.Metering{
			Bucket: "bucket",
		},
	}, lg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.Close())
	})

	// Replace the S3 writer with the local writer.
	localConfig := &storage.ProviderConfig{
		Type: storage.ProviderTypeLocalFS,
		LocalFS: &storage.LocalFSConfig{
			BasePath:   dir,
			CreateDirs: true,
		},
	}
	provider, err := storage.NewObjectStorageProvider(localConfig)
	require.NoError(t, err)
	meteringConfig := mconfig.DefaultConfig().WithLogger(lg)
	m.writer = meteringwriter.NewMeteringWriter(provider, meteringConfig)
	reader := meteringreader.NewMeteringReader(provider, meteringConfig)
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
	})
	return m, reader
}

func readMeteringData(t *testing.T, reader *meteringreader.MeteringReader, ts int64) []map[string]any {
	_, err := reader.ListFilesByTimestamp(context.Background(), ts)
	require.NoError(t, err)

	categories, err := reader.GetCategories(context.Background(), ts)
	require.NoError(t, err)
	if len(categories) == 0 {
		return nil
	}

	category := categories[0]
	categoryFiles, err := reader.GetFilesByCategory(context.Background(), ts, category)
	require.NoError(t, err)
	if len(categoryFiles) == 0 {
		return nil
	}

	filePath := categoryFiles[0]
	meteringData, err := reader.ReadFile(context.Background(), filePath)
	require.NoError(t, err)
	return meteringData.Data
}

func getValuesFromData(t *testing.T, data []map[string]any, clusterID string) (int64, int64) {
	for i := range data {
		if data[i]["cluster_id"] == clusterID {
			outBound, ok := data[i]["outBound_bytes"].(map[string]any)
			require.True(t, ok)
			crossZone, ok := data[i]["crossZone_bytes"].(map[string]any)
			require.True(t, ok)
			require.Equal(t, "bytes", outBound["unit"])
			require.Equal(t, "bytes", crossZone["unit"])
			return int64(outBound["value"].(float64)), int64(crossZone["value"].(float64))
		}
	}
	return 0, 0
}
