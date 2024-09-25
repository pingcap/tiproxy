// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStartAndStop(t *testing.T) {
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, nil)
	defer mgr.Close()
	cpt, rep := &mockCapture{}, &mockReplay{}
	mgr.capture, mgr.replay = cpt, rep

	require.Contains(t, mgr.Stop(), "no job running")
	require.NotNil(t, mgr.GetCapture())

	require.NoError(t, mgr.StartCapture(capture.CaptureConfig{}))
	require.Error(t, mgr.StartCapture(capture.CaptureConfig{}))
	require.Error(t, mgr.StartReplay(replay.ReplayConfig{}))
	require.Len(t, mgr.jobHistory, 1)
	require.NotEmpty(t, mgr.Jobs())
	require.Contains(t, mgr.Stop(), "stopped")
	require.Contains(t, mgr.Stop(), "no job running")
	require.Len(t, mgr.jobHistory, 1)

	require.NoError(t, mgr.StartReplay(replay.ReplayConfig{}))
	require.Error(t, mgr.StartCapture(capture.CaptureConfig{}))
	require.Error(t, mgr.StartReplay(replay.ReplayConfig{}))
	require.Len(t, mgr.jobHistory, 2)
	require.Contains(t, mgr.Stop(), "stopped")
	require.Contains(t, mgr.Stop(), "no job running")
	require.Len(t, mgr.jobHistory, 2)

	// Test that Jobs() also update progress.
	require.NoError(t, mgr.StartReplay(replay.ReplayConfig{}))
	rep.progress = 1.0
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
	mgr := NewJobManager(zap.NewNop(), &config.Config{}, &mockCertMgr{}, nil)
	mgr.jobHistory = []Job{
		&captureJob{
			job: job{
				startTime: startTime,
				endTime:   endTime,
				progress:  0.5,
				err:       errors.New("mock error"),
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
		},
		&replayJob{
			job: job{
				startTime: startTime,
				endTime:   endTime,
				progress:  1,
			},
			cfg: replay.ReplayConfig{
				Input:    "/tmp/traffic",
				Username: "root",
				Speed:    0.5,
			},
		},
	}
	t.Log(mgr.Jobs())
	require.Equal(t, `[
  {
    "type": "capture",
    "status": "canceled",
    "start_time": "2020-01-01 00:00:00 +0000 UTC",
    "end_time": "2020-01-01 02:01:01 +0000 UTC",
    "duration": "2h0m0s",
    "output": "/tmp/traffic",
    "progress": "50%",
    "error": "mock error"
  },
  {
    "type": "replay",
    "status": "running",
    "start_time": "2020-01-01 00:00:00 +0000 UTC",
    "input": "/tmp/traffic",
    "username": "root",
    "speed": 1,
    "progress": "0%"
  },
  {
    "type": "replay",
    "status": "done",
    "start_time": "2020-01-01 00:00:00 +0000 UTC",
    "end_time": "2020-01-01 02:01:01 +0000 UTC",
    "input": "/tmp/traffic",
    "username": "root",
    "speed": 0.5,
    "progress": "100%"
  }
]`, mgr.Jobs())
}
