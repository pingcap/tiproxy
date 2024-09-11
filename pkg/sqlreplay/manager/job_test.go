// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestJobs(t *testing.T) {
	tests := []struct {
		job Job
		tp  jobType
	}{
		{
			&captureJob{
				job: job{
					StartTime: time.Now(),
					Duration:  10 * time.Second,
				},
			},
			Capture,
		},
		{
			&replayJob{
				job: job{
					StartTime: time.Now(),
					Duration:  10 * time.Second,
				},
			},
			Replay,
		},
	}

	for i, test := range tests {
		require.Equal(t, test.tp, test.job.Type(), "case %d", i)
		require.True(t, test.job.IsRunning(), "case %d", i)
		test.job.SetProgress(0.5, errors.New("stopped manually"))
		require.False(t, test.job.IsRunning(), "case %d", i)
		require.NotEmpty(t, test.job.String(), "case %d", i)
	}
}
