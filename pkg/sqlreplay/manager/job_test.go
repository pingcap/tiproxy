// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestIsRunning(t *testing.T) {
	tests := []struct {
		job     Job
		tp      jobType
		running bool
	}{
		{
			job: &captureJob{
				job: job{
					StartTime: time.Now(),
					Duration:  10 * time.Second,
				},
			},
			tp:      Capture,
			running: true,
		},
		{
			job: &replayJob{
				job: job{
					StartTime: time.Now(),
					Duration:  10 * time.Second,
				},
			},
			tp:      Replay,
			running: true,
		},
		{
			job: &captureJob{
				job: job{
					StartTime: time.Now().Add(-5 * time.Second),
					Duration:  10 * time.Second,
					Progress:  0.5,
					Error:     errors.New("stopped manually"),
				},
			},
			tp:      Capture,
			running: false,
		},
		{
			job: &replayJob{
				job: job{
					StartTime: time.Now().Add(-20 * time.Second),
					Duration:  10 * time.Second,
					Progress:  1.0,
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
				StartTime: time.Now(),
				Duration:  10 * time.Second,
			},
		}
		job.SetProgress(test.progress, test.err)
		require.Equal(t, test.expectedProgress, job.Progress, "case %d", i)
		require.Equal(t, test.running, job.IsRunning(), "case %d", i)
	}
}
