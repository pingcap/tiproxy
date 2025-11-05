// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStartAndStop(t *testing.T) {
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil)
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
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, id.NewIDManager(), nil)
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
  }
]`, mgr.Jobs())
}

func TestHistoryLen(t *testing.T) {
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, nil, nil)
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
