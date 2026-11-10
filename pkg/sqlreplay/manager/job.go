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
	SetProgress(progress float64, endTime time.Time, done bool, err error)
	IsRunning() bool
}

type job struct {
	startTime time.Time
	endTime   time.Time
	progress  float64
	err       error
	done      bool
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

func (job *job) SetProgress(progress float64, endTime time.Time, done bool, err error) {
	if progress > job.progress {
		job.progress = progress
	}
	job.endTime = endTime
	job.err = err
	job.done = done
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
	} else if job.done {
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
<<<<<<< HEAD
	job4Marshal.Output = ast.RedactURL(job.cfg.Output)
	job4Marshal.Duration = job.cfg.Duration.String()
	return json.Marshal(job4Marshal)
=======
	c := captureJob4Marshal{
		job4Marshal:      *job4Marshal,
		Output:           replay.RedactStorageURL(job.cfg.Output),
		Duration:         job.cfg.Duration.String(),
		Compress:         job.cfg.Compress,
		EncryptionMethod: job.cfg.EncryptionMethod,
	}
	return json.Marshal(c)
>>>>>>> 89402815 (config, api, sqlreplay: redact object storage URL in gRPC and logs (#1412))
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
<<<<<<< HEAD
	job4Marshal.Input = ast.RedactURL(job.cfg.Input)
	job4Marshal.Username = job.cfg.Username
	job4Marshal.Speed = job.cfg.Speed
	if job4Marshal.Speed == 0 {
		job4Marshal.Speed = 1
=======
	r := replayJob4Marshal{
		job4Marshal: *job4Marshal,
		LastCmdTs:   job.lastCmdTs.Format(time.RFC3339Nano),
		Input:       replay.RedactStorageList(job.cfg.Input),
		Username:    job.cfg.Username,
		Speed:       job.cfg.Speed,
		ReadOnly:    job.cfg.ReadOnly,
		Format:      job.cfg.Format,
		Addr:        job.cfg.Addr,
>>>>>>> 89402815 (config, api, sqlreplay: redact object storage URL in gRPC and logs (#1412))
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
