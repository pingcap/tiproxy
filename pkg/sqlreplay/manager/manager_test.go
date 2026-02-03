// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStartAndStop(t *testing.T) {
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil, false)
	defer mgr.Close()
	cpt, rep := &mockCapture{}, &mockReplay{}
	mgr.capture, mgr.replay = cpt, rep

	require.Contains(t, mgr.Stop(CancelConfig{Type: Capture | Replay}), "no job running")
	require.NotNil(t, mgr.GetCapture())

	require.NoError(t, mgr.StartCapture(capture.CaptureConfig{}))
	require.Error(t, mgr.StartCapture(capture.CaptureConfig{}))
	require.Error(t, mgr.StartReplay(replay.ReplayConfig{}))
	require.Len(t, mgr.jobHistory, 1)
	require.NotEmpty(t, mgr.Jobs())
	require.Contains(t, mgr.Stop(CancelConfig{Type: Replay}), "no privilege to stop the job")
	require.Contains(t, mgr.Stop(CancelConfig{Type: Capture}), "stopped")
	require.Contains(t, mgr.Stop(CancelConfig{Type: Capture}), "no job running")
	require.Len(t, mgr.jobHistory, 1)

	require.NoError(t, mgr.StartReplay(replay.ReplayConfig{}))
	require.Error(t, mgr.StartCapture(capture.CaptureConfig{}))
	require.Error(t, mgr.StartReplay(replay.ReplayConfig{}))
	require.Len(t, mgr.jobHistory, 2)
	require.Contains(t, mgr.Stop(CancelConfig{Type: Capture}), "no privilege to stop the job")
	require.Contains(t, mgr.Stop(CancelConfig{Type: Replay}), "stopped")
	require.Contains(t, mgr.Stop(CancelConfig{Type: Replay}), "no job running")
	require.Len(t, mgr.jobHistory, 2)

	// Test that Jobs() also update progress.
	require.NoError(t, mgr.StartReplay(replay.ReplayConfig{}))
	rep.progress = 1.0
	rep.done = true
	mgr.Jobs()
	job := mgr.jobHistory[len(mgr.jobHistory)-1]
	require.Equal(t, 1.0, job.(*replayJob).progress)
	require.NoError(t, mgr.StartCapture(capture.CaptureConfig{}))
	cpt.err = errors.Errorf("mock error")
	mgr.Jobs()
	job = mgr.jobHistory[len(mgr.jobHistory)-1]
	require.NotNil(t, job)
	require.ErrorContains(t, job.(*captureJob).err, "mock error")
}

func TestMarshalJobHistory(t *testing.T) {
	startTime, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
	require.NoError(t, err)
	endTime, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 02:01:01")
	require.NoError(t, err)
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil, false)
	mgr.jobHistory = []Job{
		&captureJob{
			job: job{
				startTime: startTime,
				endTime:   endTime,
				progress:  0.5,
				err:       errors.New("mock error"),
				done:      true,
			},
			cfg: capture.CaptureConfig{
				Output:   "/tmp/traffic",
				Duration: 2 * time.Hour,
			},
		},
		&replayJob{
			job: job{
				startTime: startTime,
				progress:  0,
			},
			cfg: replay.ReplayConfig{
				Input:    "/tmp/traffic",
				Username: "root",
			},
			lastCmdTs: endTime,
		},
		&replayJob{
			job: job{
				startTime: startTime,
				endTime:   endTime,
				progress:  1,
				done:      true,
			},
			cfg: replay.ReplayConfig{
				Input:    "/tmp/traffic",
				Username: "root",
				Speed:    0.5,
			},
			lastCmdTs: endTime,
		},
		&replayJob{
			job: job{
				startTime: startTime,
				endTime:   endTime,
				progress:  0,
				done:      true,
			},
			cfg: replay.ReplayConfig{
				Input:    "/tmp/traffic",
				Username: "root",
				Addr:     "127.0.0.100:10000",
			},
			lastCmdTs: endTime,
		},
	}
	t.Log(mgr.Jobs())
	require.Equal(t, `[
  {
    "type": "capture",
    "status": "canceled",
    "start_time": "2020-01-01T00:00:00Z",
    "end_time": "2020-01-01T02:01:01Z",
    "progress": "50%",
    "error": "mock error",
    "output": "/tmp/traffic",
    "duration": "2h0m0s"
  },
  {
    "type": "replay",
    "status": "running",
    "start_time": "2020-01-01T00:00:00Z",
    "progress": "0%",
    "last_cmd_ts": "2020-01-01T02:01:01Z",
    "input": "/tmp/traffic",
    "username": "root"
  },
  {
    "type": "replay",
    "status": "done",
    "start_time": "2020-01-01T00:00:00Z",
    "end_time": "2020-01-01T02:01:01Z",
    "progress": "100%",
    "last_cmd_ts": "2020-01-01T02:01:01Z",
    "input": "/tmp/traffic",
    "username": "root",
    "speed": 0.5
  },
  {
    "type": "replay",
    "status": "done",
    "start_time": "2020-01-01T00:00:00Z",
    "end_time": "2020-01-01T02:01:01Z",
    "progress": "0%",
    "last_cmd_ts": "2020-01-01T02:01:01Z",
    "input": "/tmp/traffic",
    "username": "root",
    "addr": "127.0.0.100:10000"
  }
]`, mgr.Jobs())
}

func TestHistoryLen(t *testing.T) {
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, nil, nil, false)
	defer mgr.Close()
	cpt, rep := &mockCapture{}, &mockReplay{}
	mgr.capture, mgr.replay = cpt, rep
	require.Len(t, mgr.jobHistory, 0)

	for i := range maxJobHistoryCount + 1 {
		require.NoError(t, mgr.StartCapture(capture.CaptureConfig{}))
		require.Contains(t, mgr.Stop(CancelConfig{Type: Capture}), "stopped")
		expectedLen := min(i+1, maxJobHistoryCount)
		require.Len(t, mgr.jobHistory, expectedLen)
	}
}

func TestAllowAddrOnlyForStandaloneService(t *testing.T) {
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil, false)
	defer mgr.Close()

	err := mgr.StartReplay(replay.ReplayConfig{
		Addr: "127.0.0.100:10000",
	})
	require.ErrorContains(t, err, "Addr is not allowed in replay config in a TiProxy node")

	mgr2 := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil, true)
	defer mgr2.Close()
	mgr2.capture, mgr2.replay = &mockCapture{}, &mockReplay{}

	err = mgr2.StartReplay(replay.ReplayConfig{
		Addr: "127.0.0.100:10000",
	})
	require.NoError(t, err)
}

func TestAvoidConcurrentReplay(t *testing.T) {
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil, true)
	defer mgr.Close()
	mgr.replay = &mockReplay{}

	successCount := 0
	attempts := 1000

	wg := &sync.WaitGroup{}
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := mgr.StartReplay(replay.ReplayConfig{
				Addr:            "127.0.0.1:10000",
				Input:           "/tmp/traffic",
				Username:        "root",
				StartTime:       time.Now(),
				PSCloseStrategy: cmd.PSCloseStrategyAlways,
			})
			if err == nil {
				successCount++
			} else {
				require.ErrorContains(t, err, "a job is running")
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, successCount)
}

func TestGracefulCancelTimeout(t *testing.T) {
	mgr, backend := startBlockingReplay(t)
	defer mgr.Close()
	defer backend.Close()

	timeout := 200 * time.Millisecond
	start := time.Now()
	result := mgr.Stop(CancelConfig{Type: Replay, Graceful: true, GracefulTimeout: timeout})
	require.Contains(t, result, "graceful cancel timeout")
	require.Less(t, time.Since(start), timeout*10)

	_, _, _, _, done, _ := mgr.replay.Progress()
	require.False(t, done, "replay finished before force cancel")

	backend.Close()
	result = mgr.Stop(CancelConfig{Type: Replay, Graceful: false})
	require.Contains(t, result, "stopped")

	_, _, _, _, done, _ = mgr.replay.Progress()
	require.True(t, done)
}

func TestShowDuringGracefulCancel(t *testing.T) {
	mgr, backend := startBlockingReplay(t)
	defer mgr.Close()
	defer backend.Close()

	gracefulCancelTimeout := time.Second
	stopDone := make(chan struct{})
	go func() {
		_ = mgr.Stop(CancelConfig{Type: Replay, Graceful: true, GracefulTimeout: gracefulCancelTimeout})
		close(stopDone)
	}()

	time.Sleep(gracefulCancelTimeout / 10)
	select {
	case <-stopDone:
		t.Fatal("graceful cancel returned early")
	default:
	}

	showDone := make(chan struct{})
	go func() {
		_ = mgr.Jobs()
		close(showDone)
	}()
	select {
	case <-showDone:
	case <-time.After(gracefulCancelTimeout):
		t.Fatal("show blocked during graceful cancel")
	}

	select {
	case <-stopDone:
	case <-time.After(gracefulCancelTimeout * 2):
		t.Fatal("graceful cancel did not return")
	}

	backend.Close()
	_ = mgr.Stop(CancelConfig{Type: Replay, Graceful: false})
}

func startBlockingReplay(t *testing.T) (*jobManager, *silentBackend) {
	t.Helper()

	backend := newSilentBackend(t)
	dir := t.TempDir()
	writeNativeLog(t, dir)

	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil, true)
	cfg := replay.ReplayConfig{
		Input:           dir,
		Format:          cmd.FormatNative,
		Username:        "root",
		ReadOnly:        true,
		StartTime:       time.Now().Add(-time.Second),
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
		Addr:            backend.Addr(),
	}
	require.NoError(t, mgr.StartReplay(cfg))
	backend.WaitAccepted(t, time.Second)
	return mgr, backend
}

func writeNativeLog(t *testing.T, dir string) {
	t.Helper()

	command := cmd.NewCommand(pnet.MakeQueryPacket("select 1"), time.Now(), 1)
	encoder := cmd.NewCmdEncoder(cmd.FormatNative)
	var buf bytes.Buffer
	require.NoError(t, encoder.Encode(command, &buf))

	fileName := fmt.Sprintf("traffic-%s.log", time.Now().In(time.Local).Format("2006-01-02T15-04-05.999"))
	path := filepath.Join(dir, fileName)
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))
}

type silentBackend struct {
	listener     net.Listener
	acceptedOnce sync.Once
	acceptedCh   chan struct{}
	mu           sync.Mutex
	conns        []net.Conn
}

func newSilentBackend(t *testing.T) *silentBackend {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backend := &silentBackend{
		listener:   listener,
		acceptedCh: make(chan struct{}),
	}
	go backend.acceptLoop()
	return backend
}

func (b *silentBackend) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		b.conns = append(b.conns, conn)
		b.mu.Unlock()
		b.acceptedOnce.Do(func() {
			close(b.acceptedCh)
		})
	}
}

func (b *silentBackend) Addr() string {
	return b.listener.Addr().String()
}

func (b *silentBackend) WaitAccepted(t *testing.T, timeout time.Duration) {
	t.Helper()

	select {
	case <-b.acceptedCh:
	case <-time.After(timeout):
		t.Fatal("backend did not accept connection")
	}
}

func (b *silentBackend) CloseConns() {
	b.mu.Lock()
	conns := append([]net.Conn(nil), b.conns...)
	b.conns = nil
	b.mu.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (b *silentBackend) Close() {
	_ = b.listener.Close()
	b.CloseConns()
}
