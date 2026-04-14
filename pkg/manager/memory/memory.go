// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

const (
	// Check the memory usage every 5 seconds.
	checkInterval = 5 * time.Second
	// No need to record too frequently.
	recordMinInterval = 5 * time.Minute
	// Record the profiles when the memory usage is higher than 60%.
	alarmThreshold = 0.6
	// Remove the oldest profiles when the number of profiles exceeds this limit.
	maxSavedProfiles = 20
	// Fail open if the latest sampled usage is too old.
	snapshotExpireInterval = 2 * checkInterval
)

type UsageSnapshot struct {
	Used                   uint64
	Limit                  uint64
	Usage                  float64
	UpdateTime             time.Time
	EstimatedConnBufferMem int64
	Valid                  bool
}

// MemManager is a manager for memory usage.
// Although the continuous profiling collects profiles periodically, when TiProxy runs in the replayer mode,
// the profiles are not collected.
type MemManager struct {
	lg                *zap.Logger
	cancel            context.CancelFunc
	wg                waitgroup.WaitGroup
	cfgGetter         config.ConfigGetter
	savedProfileNames []string
	lastRecordTime    time.Time
	checkInterval     time.Duration // used for test
	recordMinInterval time.Duration // used for test
	maxSavedProfiles  int           // used for test
	snapshotExpire    time.Duration // used for test
	memoryLimit       uint64
	latestUsage       atomic.Value
	connBufferMem     atomic.Int64
}

func NewMemManager(lg *zap.Logger, cfgGetter config.ConfigGetter) *MemManager {
	mgr := &MemManager{
		lg:                lg,
		cfgGetter:         cfgGetter,
		checkInterval:     checkInterval,
		recordMinInterval: recordMinInterval,
		maxSavedProfiles:  maxSavedProfiles,
		snapshotExpire:    snapshotExpireInterval,
	}
	mgr.latestUsage.Store(UsageSnapshot{})
	return mgr
}

func (m *MemManager) Start(ctx context.Context) {
	// Call the memory.MemTotal and memory.MemUsed in TiDB repo because they have considered cgroup.
	limit, err := memory.MemTotal()
	if err != nil || limit == 0 {
		m.lg.Error("get memory limit failed", zap.Uint64("limit", limit), zap.Error(err))
		return
	}
	m.memoryLimit = limit
	if _, err = m.refreshUsage(); err != nil {
		return
	}
	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.wg.RunWithRecover(func() {
		m.alarmLoop(childCtx)
	}, nil, m.lg)
}

func (m *MemManager) alarmLoop(ctx context.Context) {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAndAlarm()
		}
	}
}

func (m *MemManager) checkAndAlarm() {
	snapshot, err := m.refreshUsage()
	if err != nil || !snapshot.Valid {
		return
	}
	if snapshot.Usage < alarmThreshold {
		return
	}
	if time.Since(m.lastRecordTime) < m.recordMinInterval {
		return
	}
	// The filename is hot-reloadable.
	cfg := m.cfgGetter.GetConfig()
	if cfg == nil {
		return
	}
	logPath := cfg.Log.LogFile.Filename
	if logPath == "" {
		return
	}
	recordDir := filepath.Dir(logPath)

	m.lastRecordTime = snapshot.UpdateTime
	m.lg.Warn("memory usage alarm", zap.Uint64("limit", snapshot.Limit), zap.Uint64("used", snapshot.Used), zap.Float64("usage", snapshot.Usage))
	now := time.Now().Format(time.RFC3339)
	m.recordHeap(filepath.Join(recordDir, "heap_"+now))
	m.recordGoroutine(filepath.Join(recordDir, "goroutine_"+now))
	m.rmExpiredProfiles()
}

func (m *MemManager) refreshUsage() (UsageSnapshot, error) {
	if m.memoryLimit == 0 {
		return UsageSnapshot{}, nil
	}
	used, err := memory.MemUsed()
	if err != nil || used == 0 {
		m.lg.Error("get used memory failed", zap.Uint64("used", used), zap.Error(err))
		return UsageSnapshot{}, err
	}
	snapshot := UsageSnapshot{
		Used:                   used,
		Limit:                  m.memoryLimit,
		Usage:                  float64(used) / float64(m.memoryLimit),
		UpdateTime:             time.Now(),
		EstimatedConnBufferMem: m.connBufferMem.Load(),
		Valid:                  true,
	}
	m.latestUsage.Store(snapshot)
	return snapshot, nil
}

func (m *MemManager) LatestUsage() UsageSnapshot {
	snapshot, _ := m.latestUsage.Load().(UsageSnapshot)
	return snapshot
}

func (m *MemManager) UpdateConnBufferMemory(delta int64) {
	if m == nil || delta == 0 {
		return
	}
	for {
		current := m.connBufferMem.Load()
		next := current + delta
		if next < 0 {
			next = 0
		}
		if m.connBufferMem.CompareAndSwap(current, next) {
			return
		}
	}
}

func (m *MemManager) adjustUsageByConnBuffer(snapshot UsageSnapshot) UsageSnapshot {
	current := m.connBufferMem.Load()
	delta := current - snapshot.EstimatedConnBufferMem
	snapshot.EstimatedConnBufferMem = current
	if delta == 0 {
		return snapshot
	}
	if delta > 0 {
		snapshot.Used += uint64(delta)
	} else {
		released := uint64(-delta)
		if released >= snapshot.Used {
			snapshot.Used = 0
		} else {
			snapshot.Used -= released
		}
	}
	if snapshot.Limit > 0 {
		snapshot.Usage = float64(snapshot.Used) / float64(snapshot.Limit)
	}
	return snapshot
}

func (m *MemManager) ShouldRejectNewConn() (bool, UsageSnapshot, float64) {
	if m == nil || m.cfgGetter == nil {
		return false, UsageSnapshot{}, 0
	}
	cfg := m.cfgGetter.GetConfig()
	if cfg == nil {
		return false, UsageSnapshot{}, 0
	}
	threshold := cfg.Proxy.HighMemoryUsageRejectThreshold
	if threshold == 0 {
		return false, UsageSnapshot{}, 0
	}
	snapshot := m.LatestUsage()
	if !snapshot.Valid || time.Since(snapshot.UpdateTime) > m.snapshotExpire {
		return false, snapshot, threshold
	}
	snapshot = m.adjustUsageByConnBuffer(snapshot)
	return snapshot.Usage >= threshold, snapshot, threshold
}

func (m *MemManager) recordHeap(fileName string) {
	f, err := os.Create(fileName)
	if err != nil {
		m.lg.Error("failed to create heap profile file", zap.Error(err))
		return
	}
	defer f.Close()
	p := pprof.Lookup("heap")
	if err = p.WriteTo(f, 0); err != nil {
		m.lg.Error("failed to write heap profile file", zap.Error(err))
	}
	m.savedProfileNames = append(m.savedProfileNames, fileName)
}

func (m *MemManager) recordGoroutine(fileName string) {
	buf := make([]byte, 1<<26) // 64MB buffer
	n := runtime.Stack(buf, true)
	if n >= len(buf) {
		m.lg.Warn("goroutine stack trace is too large, truncating", zap.Int("size", n))
	}
	//nolint: gosec
	if err := os.WriteFile(fileName, buf[:n], 0644); err != nil {
		m.lg.Error("failed to write goroutine profile file", zap.Error(err))
	}
	m.savedProfileNames = append(m.savedProfileNames, fileName)
}

func (m *MemManager) rmExpiredProfiles() {
	for len(m.savedProfileNames) > m.maxSavedProfiles {
		if err := os.Remove(m.savedProfileNames[0]); err != nil {
			m.lg.Warn("failed to remove expired profile file", zap.String("file", m.savedProfileNames[0]), zap.Error(err))
		}
		copy(m.savedProfileNames[0:], m.savedProfileNames[1:])
		m.savedProfileNames = m.savedProfileNames[:len(m.savedProfileNames)-1]
	}
}

func (m *MemManager) Close() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}
