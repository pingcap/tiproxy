// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/siddontang/go/hack"
)

type jobType int

const (
	Capture jobType = iota
	Replay
)

type Job interface {
	Type() jobType
	String() string
	MarshalJSON() ([]byte, error)
	SetProgress(progress float64, endTime time.Time, err error)
	IsRunning() bool
}

type job struct {
	startTime time.Time
	endTime   time.Time
	progress  float64
	err       error
}

type job4Marshal struct {
	Type      string  `json:"type"`
	Status    string  `json:"status"`
	StartTime string  `json:"start_time"`
	EndTime   string  `json:"end_time,omitempty"`
	Duration  string  `json:"duration,omitempty"`
	Output    string  `json:"output,omitempty"`
	Input     string  `json:"input,omitempty"`
	Username  string  `json:"username,omitempty"`
	Speed     float64 `json:"speed,omitempty"`
	Progress  string  `json:"progress"`
	Err       string  `json:"error,omitempty"`
}

func (job *job) IsRunning() bool {
	return job.err == nil && job.progress < 1
}

func (job *job) SetProgress(progress float64, endTime time.Time, err error) {
	if progress > job.progress {
		job.progress = progress
	}
	job.endTime = endTime
	job.err = err
}

func (job *job) getJob4Marshal() *job4Marshal {
	jm := &job4Marshal{
		StartTime: job.startTime.String(),
		Progress:  fmt.Sprintf("%d%%", int(job.progress*100)),
	}
	if !job.endTime.IsZero() {
		jm.EndTime = job.endTime.String()
	}
	if job.err != nil {
		jm.Status = "canceled"
		jm.Err = job.err.Error()
	} else if job.progress >= 1.0 {
		jm.Status = "done"
	} else {
		jm.Status = "running"
	}
	return jm
}

var _ Job = (*captureJob)(nil)

type captureJob struct {
	job
	cfg capture.CaptureConfig
}

func (job *captureJob) Type() jobType {
	return Capture
}

func (job *captureJob) MarshalJSON() ([]byte, error) {
	job4Marshal := job.getJob4Marshal()
	job4Marshal.Type = "capture"
	job4Marshal.Output = job.cfg.Output
	job4Marshal.Duration = job.cfg.Duration.String()
	return json.Marshal(job4Marshal)
}

func (job *captureJob) String() string {
	b, err := json.Marshal(job)
	if err != nil {
		return ""
	}
	return hack.String(b)
}

var _ Job = (*replayJob)(nil)

type replayJob struct {
	job
	cfg replay.ReplayConfig
}

func (job *replayJob) Type() jobType {
	return Replay
}

func (job *replayJob) MarshalJSON() ([]byte, error) {
	job4Marshal := job.getJob4Marshal()
	job4Marshal.Type = "replay"
	job4Marshal.Input = job.cfg.Input
	job4Marshal.Username = job.cfg.Username
	job4Marshal.Speed = job.cfg.Speed
	if job4Marshal.Speed == 0 {
		job4Marshal.Speed = 1
	}
	return json.Marshal(job4Marshal)
}

func (job *replayJob) String() string {
	b, err := json.Marshal(job)
	if err != nil {
		return ""
	}
	return hack.String(b)
}