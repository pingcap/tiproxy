// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestManageConns(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	replay := NewReplay(lg, nil, nil, nil)
	defer replay.Close()
	replay.connCreator = func(connID uint64, exceptionCh chan Exception, closeCh chan uint64) Conn {
		return &mockConn{
			exceptionCh: exceptionCh,
			closeCh:     closeCh,
			connID:      connID,
		}
	}
	loader := newMockChLoader()
	require.NoError(t, replay.Start(ReplayConfig{
		Input:    t.TempDir(),
		Username: "u1",
		reader:   loader,
	}))

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

func TestReadExceptions(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	replay := NewReplay(lg, nil, nil, nil)
	defer replay.Close()

	dir := t.TempDir()
	require.NoError(t, replay.Start(ReplayConfig{
		Input:    dir,
		Username: "u1",
	}))
	replay.exceptionCh <- otherException{err: errors.New("mock error"), connID: 1}
	require.Eventually(t, func() bool {
		return strings.Contains(text.String(), "mock error")
	}, 3*time.Second, 10*time.Millisecond)
}

func TestReplaySpeed(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	speeds := []float64{10, 1, 0.1}

	var lastTotalTime time.Duration
	for _, speed := range speeds {
		replay := NewReplay(lg, nil, nil, nil)
		cmdCh := make(chan *cmd.Command, 10)
		replay.connCreator = func(connID uint64, exceptionCh chan Exception, closeCh chan uint64) Conn {
			return &mockConn{
				exceptionCh: exceptionCh,
				closeCh:     closeCh,
				connID:      connID,
				cmdCh:       cmdCh,
			}
		}

		loader := newMockNormalLoader()
		now := time.Now()
		for i := 0; i < 10; i++ {
			command := newMockCommand(1)
			command.StartTs = now.Add(time.Duration(i*10) * time.Millisecond)
			loader.writeCommand(command)
		}
		require.NoError(t, replay.Start(ReplayConfig{
			Input:    t.TempDir(),
			Username: "u1",
			reader:   loader,
			Speed:    speed,
		}))

		var firstTime, lastTime time.Time
		for i := 0; i < 10; i++ {
			<-cmdCh
			if lastTime.IsZero() {
				firstTime = time.Now()
				lastTime = firstTime
			} else {
				now = time.Now()
				interval := now.Sub(lastTime)
				lastTime = now
				require.Greater(t, interval, time.Duration(float64(10*time.Millisecond)/speed)/2, "speed: %f, i: %d", speed, i)
			}
		}
		totalTime := lastTime.Sub(firstTime)
		require.Greater(t, totalTime, lastTotalTime, "speed: %f", speed)
		lastTotalTime = totalTime
		replay.Close()
	}
}
