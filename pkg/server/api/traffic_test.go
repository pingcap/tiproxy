// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/manager"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/stretchr/testify/require"
)

func TestTraffic(t *testing.T) {
	server, doHTTP := createServer(t)
	mgr := server.mgr.ReplayJobMgr.(*mockReplayJobManager)

	doHTTP(t, http.MethodPost, "/api/traffic/capture?duration=1h&output=/tmp", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "capture started", string(all))
		require.Equal(t, "capture", mgr.curJob)
		require.Equal(t, capture.CaptureConfig{Duration: time.Hour, Output: "/tmp"}, mgr.captureCfg)
	})
	doHTTP(t, http.MethodPost, "/api/traffic/replay?input=/tmp", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusInternalServerError, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "job is running", string(all))
	})
	doHTTP(t, http.MethodPost, "/api/traffic/cancel", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "stopped", string(all))
		require.Equal(t, "", mgr.curJob)
	})
	doHTTP(t, http.MethodPost, "/api/traffic/replay?input=/tmp&speed=2.0", httpOpts{form: map[string]string{"username": "u1"}, header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"}}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "replay started", string(all))
		require.Equal(t, "replay", mgr.curJob)
		require.Equal(t, replay.ReplayConfig{Input: "/tmp", Username: "u1", Speed: 2.0}, mgr.replayCfg)
	})
	doHTTP(t, http.MethodGet, "/api/traffic/show", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "replay", string(all))
	})
	doHTTP(t, http.MethodPost, "/api/traffic/cancel", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
}

var _ manager.JobManager = (*mockReplayJobManager)(nil)

type mockReplayJobManager struct {
	curJob     string
	captureCfg capture.CaptureConfig
	replayCfg  replay.ReplayConfig
}

func (m *mockReplayJobManager) Close() {
}

func (m *mockReplayJobManager) GetCapture() capture.Capture {
	return nil
}

func (m *mockReplayJobManager) Jobs() string {
	return m.curJob
}

func (m *mockReplayJobManager) StartCapture(captureCfg capture.CaptureConfig) error {
	if m.curJob != "" {
		return errors.New("job is running")
	}
	m.captureCfg = captureCfg
	m.curJob = "capture"
	return nil
}

func (m *mockReplayJobManager) StartReplay(replayCfg replay.ReplayConfig) error {
	if m.curJob != "" {
		return errors.New("job is running")
	}
	m.replayCfg = replayCfg
	m.curJob = "replay"
	return nil
}

func (m *mockReplayJobManager) Stop() string {
	m.curJob = ""
	return "stopped"
}
