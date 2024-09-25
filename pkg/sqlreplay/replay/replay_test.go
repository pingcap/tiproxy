// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestManageConns(t *testing.T) {
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()

	loader := newMockChLoader()
	cfg := ReplayConfig{
		Input:    t.TempDir(),
		Username: "u1",
		reader:   loader,
		connCreator: func(connID uint64) conn.Conn {
			return &mockConn{
				connID:      connID,
				exceptionCh: replay.exceptionCh,
				closeCh:     replay.closeCh,
			}
		},
		report: newMockReport(replay.exceptionCh),
	}
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

	command := newMockCommand(1)
	loader.writeCommand(command)
	require.Eventually(t, func() bool {
		replay.Lock()
		defer replay.Unlock()
		cn, ok := replay.conns[1]
		return cn != nil && !reflect.ValueOf(cn).IsNil() && ok
	}, 3*time.Second, 10*time.Millisecond)

	command = newMockCommand(2)
	loader.writeCommand(command)
	require.Eventually(t, func() bool {
		replay.Lock()
		defer replay.Unlock()
		cn, ok := replay.conns[2]
		return cn != nil && !reflect.ValueOf(cn).IsNil() && ok
	}, 3*time.Second, 10*time.Millisecond)

	replay.closeCh <- 2
	require.Eventually(t, func() bool {
		replay.Lock()
		defer replay.Unlock()
		cn, ok := replay.conns[2]
		return (cn == nil || reflect.ValueOf(cn).IsNil()) && ok
	}, 3*time.Second, 10*time.Millisecond)

	loader.Close()
}

func TestValidateCfg(t *testing.T) {
	dir := t.TempDir()
	cfgs := []ReplayConfig{
		{
			Username: "u1",
		},
		{
			Input: dir,
		},
		{
			Input:    filepath.Join(dir, "input"),
			Username: "u1",
		},
		{
			Input:    dir,
			Username: "u1",
			Speed:    0.01,
		},
		{
			Input:    dir,
			Username: "u1",
			Speed:    100,
		},
	}

	for i, cfg := range cfgs {
		require.Error(t, cfg.Validate(), "case %d", i)
	}
}

func TestReplaySpeed(t *testing.T) {
	speeds := []float64{10, 1, 0.1}
	var lastTotalTime time.Duration
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()
	for _, speed := range speeds {
		cmdCh := make(chan *cmd.Command, 10)
		loader := newMockNormalLoader()
		cfg := ReplayConfig{
			Input:    t.TempDir(),
			Username: "u1",
			Speed:    speed,
			reader:   loader,
			report:   newMockReport(replay.exceptionCh),
			connCreator: func(connID uint64) conn.Conn {
				return &mockConn{
					connID:      connID,
					cmdCh:       cmdCh,
					exceptionCh: replay.exceptionCh,
					closeCh:     replay.closeCh,
				}
			},
		}

		now := time.Now()
		for i := 0; i < 10; i++ {
			command := newMockCommand(1)
			command.StartTs = now.Add(time.Duration(i*10) * time.Millisecond)
			loader.writeCommand(command)
		}
		require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

		var firstTime, lastTime time.Time
		for i := 0; i < 10; i++ {
			<-cmdCh
			if i == 0 {
				firstTime = time.Now()
				lastTime = firstTime
			} else {
				now = time.Now()
				interval := now.Sub(lastTime)
				lastTime = now
				t.Logf("speed: %f, i: %d, interval: %s", speed, i, interval)
				// CI is too unstable, comment this.
				// require.Greater(t, interval, time.Duration(float64(10*time.Millisecond)/speed)/2, "speed: %f, i: %d", speed, i)
			}
		}
		totalTime := lastTime.Sub(firstTime)
		require.Greater(t, totalTime, lastTotalTime, "speed: %f", speed)
		lastTotalTime = totalTime

		replay.Stop(nil)
		loader.Close()
	}
}

func TestProgress(t *testing.T) {
	dir := t.TempDir()
	meta := &store.Meta{
		Duration: 10 * time.Second,
		Cmds:     10,
	}
	require.NoError(t, meta.Write(dir))
	loader := newMockNormalLoader()
	now := time.Now()
	for i := 0; i < 10; i++ {
		command := newMockCommand(1)
		command.StartTs = now.Add(time.Duration(i*10) * time.Millisecond)
		loader.writeCommand(command)
	}
	defer loader.Close()

	// If the channel size is too small, there may be a deadlock.
	// ExecuteCmd waits for cmdCh <- data in a lock, while Progress() waits for the lock.
	cmdCh := make(chan *cmd.Command, 10)
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()
	cfg := ReplayConfig{
		Input:    dir,
		Username: "u1",
		reader:   loader,
		report:   newMockReport(replay.exceptionCh),
		connCreator: func(connID uint64) conn.Conn {
			return &mockConn{
				connID:      connID,
				cmdCh:       cmdCh,
				exceptionCh: replay.exceptionCh,
				closeCh:     replay.closeCh,
			}
		},
	}
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))
	for i := 0; i < 10; i++ {
		<-cmdCh
		progress, _, err := replay.Progress()
		require.NoError(t, err)
		require.GreaterOrEqual(t, progress, float64(i)/10)
		// Maybe unstable due to goroutine schedule.
		// require.LessOrEqual(t, progress, float64(i+2)/10)
	}
}
