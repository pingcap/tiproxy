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
	lg, _ := logger.CreateLoggerForTest(t)
	m, err := NewMeter(&config.Config{
		Metering: config.Metering{
			Bucket: "bucket",
		},
	}, lg)
	require.NoError(t, err)
	defer m.Close()

	// Replace the S3 writer with the local writer.
	dir := t.TempDir()
	localConfig := &storage.ProviderConfig{
		Type: storage.ProviderTypeLocalFS,
		LocalFS: &storage.LocalFSConfig{
			BasePath:   dir,
			CreateDirs: true,
		},
		Prefix: "",
	}
	provider, err := storage.NewObjectStorageProvider(localConfig)
	require.NoError(t, err)
	meteringConfig := mconfig.DefaultConfig().WithLogger(lg)
	m.writer = meteringwriter.NewMeteringWriter(provider, meteringConfig)
	reader := meteringreader.NewMeteringReader(provider, meteringConfig)
	defer reader.Close()

	ts := time.Now().Unix() / 60 * 60
	m.IncTraffic("cluster-1", 100, 200)
	m.IncTraffic("cluster-2", 200, 300)
	m.flush(context.Background(), ts)

	timestampFiles, err := reader.ListFilesByTimestamp(context.Background(), ts)
	require.NoError(t, err)
	require.Len(t, timestampFiles.Files, 1)

	categories, err := reader.GetCategories(context.Background(), ts)
	require.NoError(t, err)
	require.True(t, len(categories) > 0)

	category := categories[0]
	categoryFiles, err := reader.GetFilesByCategory(context.Background(), ts, category)
	require.NoError(t, err)
	require.True(t, len(categoryFiles) > 0)

	filePath := categoryFiles[0]
	meteringData, err := reader.ReadFile(context.Background(), filePath)
	require.NoError(t, err)

	found := []bool{false, false}
	for i := range meteringData.Data {
		if meteringData.Data[i]["cluster_id"] == "cluster-1" {
			found[0] = true
			require.Equal(t, map[string]any{"value": float64(100), "unit": "bytes"}, meteringData.Data[i]["outBound_bytes"])
			require.Equal(t, map[string]any{"value": float64(200), "unit": "bytes"}, meteringData.Data[i]["crossZone_bytes"])
		}
		if meteringData.Data[i]["cluster_id"] == "cluster-2" {
			found[1] = true
			require.Equal(t, map[string]any{"value": float64(200), "unit": "bytes"}, meteringData.Data[i]["outBound_bytes"])
			require.Equal(t, map[string]any{"value": float64(300), "unit": "bytes"}, meteringData.Data[i]["crossZone_bytes"])
		}
	}
	require.Equal(t, []bool{true, true}, found)
}
