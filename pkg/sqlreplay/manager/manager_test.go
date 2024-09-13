// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"

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
	require.Equal(t, 1.0, job.(*replayJob).Progress)
	require.NoError(t, mgr.StartCapture(capture.CaptureConfig{}))
	cpt.err = errors.Errorf("mock error")
	mgr.Jobs()
	job = mgr.jobHistory[len(mgr.jobHistory)-1]
	require.NotNil(t, job)
	require.ErrorContains(t, job.(*captureJob).Error, "mock error")
}
