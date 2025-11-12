// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockCfgGetter struct {
	cfg *config.Config
}

func (c *mockCfgGetter) GetConfig() *config.Config {
	return c.cfg
}

func TestRecordProfile(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{}
	cfg.Log.LogFile.Filename = path.Join(dir, "proxy.log")
	cfgGetter := mockCfgGetter{cfg: cfg}
	memory.MemUsed = func() (uint64, error) {
		return 9 * (1 << 30), nil
	}
	memory.MemTotal = func() (uint64, error) {
		return 10 * (1 << 30), nil
	}
	m := NewMemManager(zap.NewNop(), &cfgGetter)
	// The timestamp in file names are in seconds instead of milliseconds, so recording too frequently is useless.
	// Instead, it may overwrite the previous files.
	m.checkInterval = 100 * time.Millisecond
	m.recordMinInterval = 1200 * time.Millisecond
	m.maxSavedProfiles = 2
	m.Start(context.Background())

	// The profiles are recorded.
	require.Eventually(t, func() bool {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		prefixes := []string{"heap_", "goroutine_"}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			for i, prefix := range prefixes {
				if strings.HasPrefix(entry.Name(), prefix) {
					info, err := os.Stat(path.Join(dir, entry.Name()))
					require.NoError(t, err)
					if info.Size() == 0 {
						return false
					}
					prefixes = append(prefixes[:i], prefixes[i+1:]...)
					break
				}
			}
		}
		return len(prefixes) == 0
	}, 3*time.Second, 100*time.Millisecond)

	// The expired profiles are removed.
	time.Sleep(2 * time.Second)
	m.Close()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, m.maxSavedProfiles)
}
