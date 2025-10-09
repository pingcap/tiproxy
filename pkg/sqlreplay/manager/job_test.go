// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/stretchr/testify/require"
)

func TestIsRunning(t *testing.T) {
	tests := []struct {
		job     Job
		tp      JobType
		running bool
	}{
		{
			job: &captureJob{
				job: job{
					startTime: time.Now(),
				},
			},
			tp:      Capture,
			running: true,
		},
		{
			job: &replayJob{
				job: job{
					startTime: time.Now(),
				},
			},
			tp:      Replay,
			running: true,
		},
		{
			job: &captureJob{
				job: job{
					startTime: time.Now().Add(-5 * time.Second),
					progress:  0.5,
					err:       errors.New("stopped manually"),
				},
			},
			tp:      Capture,
			running: false,
		},
		{
			job: &replayJob{
				job: job{
					startTime: time.Now().Add(-20 * time.Second),
					progress:  0.5,
					done:      true,
				},
			},
			tp:      Replay,
			running: false,
		},
	}

	for i, test := range tests {
		require.Equal(t, test.tp, test.job.Type(), "case %d", i)
		require.Equal(t, test.running, test.job.IsRunning(), "case %d", i)
		require.NotEmpty(t, test.job.String(), "case %d", i)
	}
}

func TestSetProgress(t *testing.T) {
	tests := []struct {
		progress         float64
		err              error
		expectedProgress float64
		running          bool
	}{
		{
			progress:         0.5,
			err:              nil,
			expectedProgress: 0.5,
			running:          true,
		},
		{
			progress:         1.0,
			err:              errors.New("mock error"),
			expectedProgress: 1.0,
			running:          false,
		},
	}

	for i, test := range tests {
		job := &captureJob{
			job: job{
				startTime: time.Now(),
			},
		}
		now := time.Now()
		job.SetProgress(test.progress, now, test.err != nil || test.progress >= 1.0, test.err)
		require.Equal(t, now, job.endTime, "case %d", i)
		require.Equal(t, test.expectedProgress, job.progress, "case %d", i)
		require.Equal(t, test.running, job.IsRunning(), "case %d", i)
	}
}

func TestMarshalJob(t *testing.T) {
	startTime, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
	require.NoError(t, err)
	endTime, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 02:01:01")
	require.NoError(t, err)

	tests := []struct {
		job     Job
		marshal string
	}{
		{
			job: &captureJob{
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
			marshal: `{"type":"capture","status":"canceled","start_time":"2020-01-01T00:00:00Z","end_time":"2020-01-01T02:01:01Z","progress":"50%","error":"mock error","output":"/tmp/traffic","duration":"2h0m0s"}`,
		},
		{
			job: &captureJob{
				job: job{
					startTime: startTime,
					endTime:   endTime,
					progress:  0.5,
					done:      true,
				},
				cfg: capture.CaptureConfig{
					Output:           "s3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true",
					Duration:         2 * time.Hour,
					Compress:         true,
					EncryptionMethod: "aes256-ctr",
				},
			},
			marshal: `{"type":"capture","status":"done","start_time":"2020-01-01T00:00:00Z","end_time":"2020-01-01T02:01:01Z","progress":"50%","output":"s3://bucket/prefix?access-key=xxxxxx\u0026force-path-style=true\u0026secret-access-key=xxxxxx","duration":"2h0m0s","compress":true,"encryption-method":"aes256-ctr"}`,
		},
		{
			job: &replayJob{
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
			marshal: `{"type":"replay","status":"running","start_time":"2020-01-01T00:00:00Z","progress":"0%","last_cmd_ts":"2020-01-01T02:01:01Z","input":"/tmp/traffic","username":"root"}`,
		},
		{
			job: &replayJob{
				job: job{
					startTime: startTime,
					endTime:   endTime,
					progress:  1,
					done:      true,
				},
				cfg: replay.ReplayConfig{
					Input:    "/tmp/traffic",
					Username: "root",
					Password: "123456",
					Speed:    0.5,
					ReadOnly: true,
				},
				lastCmdTs: endTime,
			},
			marshal: `{"type":"replay","status":"done","start_time":"2020-01-01T00:00:00Z","end_time":"2020-01-01T02:01:01Z","progress":"100%","last_cmd_ts":"2020-01-01T02:01:01Z","input":"/tmp/traffic","username":"root","speed":0.5,"readonly":true}`,
		},
		{
			job: &replayJob{
				job: job{
					startTime: startTime,
					progress:  0,
				},
				cfg: replay.ReplayConfig{
					Input:    "s3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true",
					Username: "root",
				},
				lastCmdTs: endTime,
			},
			marshal: `{"type":"replay","status":"running","start_time":"2020-01-01T00:00:00Z","progress":"0%","last_cmd_ts":"2020-01-01T02:01:01Z","input":"s3://bucket/prefix?access-key=xxxxxx\u0026force-path-style=true\u0026secret-access-key=xxxxxx","username":"root"}`,
		},
	}

	for i, test := range tests {
		require.Equal(t, test.marshal, test.job.String(), "case %d", i)
	}
}
