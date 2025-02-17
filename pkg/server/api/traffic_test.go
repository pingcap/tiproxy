// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/cli"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/manager"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/stretchr/testify/require"
)

func TestTraffic(t *testing.T) {
	server, doHTTP := createServer(t)
	mgr := server.mgr.ReplayJobMgr.(*mockReplayJobManager)

	// parse duration error
	doHTTP(t, http.MethodPost, "/api/traffic/capture", httpOpts{
		reader: cli.GetFormReader(map[string]string{"output": "/tmp", "duration": "10"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "time: missing unit in duration \"10\"", string(all))
	})
	// parse start time error
	doHTTP(t, http.MethodPost, "/api/traffic/capture", httpOpts{
		reader: cli.GetFormReader(map[string]string{"output": "/tmp", "duration": "1h", "start-time": "2023-01-01"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Contains(t, string(all), "cannot parse \"\" as \"T\"")
	})
	// capture succeeds
	doHTTP(t, http.MethodPost, "/api/traffic/capture", httpOpts{
		reader: cli.GetFormReader(map[string]string{"output": "/tmp", "duration": "1h"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "capture started", string(all))
		require.Equal(t, "capture", mgr.curJob)
		startTime := mgr.captureCfg.StartTime
		require.False(t, startTime.IsZero())
		require.Equal(t, capture.CaptureConfig{Duration: time.Hour, Output: "/tmp", Compress: true, StartTime: startTime}, mgr.captureCfg)
	})
	// cancel succeeds
	doHTTP(t, http.MethodPost, "/api/traffic/cancel", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "stopped", string(all))
		require.Equal(t, "", mgr.curJob)
	})
	// capture succeeds with more options
	doHTTP(t, http.MethodPost, "/api/traffic/capture", httpOpts{
		reader: cli.GetFormReader(map[string]string{"output": "/tmp", "duration": "1h", "encrypt-method": "aes256-ctr",
			"compress": "false", "start-time": time.Now().Format(time.RFC3339)}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "capture started", string(all))
		require.Equal(t, "capture", mgr.curJob)
		require.Equal(t, capture.CaptureConfig{Duration: time.Hour, Output: "/tmp", EncryptMethod: "aes256-ctr", Compress: false,
			StartTime: mgr.captureCfg.StartTime}, mgr.captureCfg)
	})
	// job is running error
	doHTTP(t, http.MethodPost, "/api/traffic/replay", httpOpts{
		reader: cli.GetFormReader(map[string]string{"input": "/tmp"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusInternalServerError, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "job is running", string(all))
	})
	cancelJob(t, doHTTP)
	// parse speed error
	doHTTP(t, http.MethodPost, "/api/traffic/replay", httpOpts{
		reader: cli.GetFormReader(map[string]string{"input": "/tmp", "speed": "abc"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "strconv.ParseFloat: parsing \"abc\": invalid syntax", string(all))
	})
	// replay succeeds
	doHTTP(t, http.MethodPost, "/api/traffic/replay", httpOpts{
		reader: cli.GetFormReader(map[string]string{"input": "/tmp", "speed": "2.0", "username": "u1", "password": "p1"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "replay started", string(all))
		require.Equal(t, "replay", mgr.curJob)
		startTime := mgr.replayCfg.StartTime
		require.False(t, startTime.IsZero())
		require.Equal(t, replay.ReplayConfig{Input: "/tmp", Username: "u1", Password: "p1", Speed: 2.0, StartTime: startTime}, mgr.replayCfg)
	})
	// show succeeds
	doHTTP(t, http.MethodGet, "/api/traffic/show", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "replay", string(all))
	})
	cancelJob(t, doHTTP)
}

func cancelJob(t *testing.T, doHTTP doHTTPFunc) {
	doHTTP(t, http.MethodPost, "/api/traffic/cancel", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
}

func TestDisableTrafficReplay(t *testing.T) {
	server, doHTTP := createServer(t)
	server.mgr.CfgMgr.GetConfig().EnableTrafficReplay = false

	doHTTP(t, http.MethodPost, "/api/traffic/capture", httpOpts{
		reader: cli.GetFormReader(map[string]string{"output": "/tmp", "duration": "1h"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "traffic capture is disabled", string(all))
	})
	doHTTP(t, http.MethodPost, "/api/traffic/replay", httpOpts{
		reader: cli.GetFormReader(map[string]string{"input": "/tmp"}),
		header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "traffic replay is disabled", string(all))
	})
	doHTTP(t, http.MethodPost, "/api/traffic/cancel", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "traffic cancel is disabled", string(all))
	})
	doHTTP(t, http.MethodGet, "/api/traffic/show", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "traffic show is disabled", string(all))
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

func (m *mockReplayJobManager) Stop(manager.CancelConfig) string {
	m.curJob = ""
	return "stopped"
}
