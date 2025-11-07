// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/siddontang/go/hack"
)

type JobType int

const (
	Capture JobType = 1 << iota
	Replay
)

type Job interface {
	Type() JobType
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
	Type      string `json:"type"`
	Status    string `json:"status"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time,omitempty"`
	Progress  string `json:"progress"`
	Err       string `json:"error,omitempty"`
}

func (job *job) IsRunning() bool {
	return job.err == nil && !job.done
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
		StartTime: job.startTime.Format(time.RFC3339),
		Progress:  fmt.Sprintf("%d%%", int(job.progress*100)),
	}
	if !job.endTime.IsZero() {
		jm.EndTime = job.endTime.Format(time.RFC3339)
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

type captureJob4Marshal struct {
	job4Marshal
	Output           string `json:"output,omitempty"`
	Duration         string `json:"duration,omitempty"`
	Compress         bool   `json:"compress,omitempty"`
	EncryptionMethod string `json:"encryption-method,omitempty"`
}

func (job *captureJob) Type() JobType {
	return Capture
}

func (job *captureJob) MarshalJSON() ([]byte, error) {
	job4Marshal := job.getJob4Marshal()
	job4Marshal.Type = "capture"
	c := captureJob4Marshal{
		job4Marshal:      *job4Marshal,
		Output:           ast.RedactURL(job.cfg.Output),
		Duration:         job.cfg.Duration.String(),
		Compress:         job.cfg.Compress,
		EncryptionMethod: job.cfg.EncryptionMethod,
	}
	return json.Marshal(c)
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
	cfg          replay.ReplayConfig
	lastCmdTs    time.Time
	lastCmdEndTs time.Time
}

type replayJob4Marshal struct {
	job4Marshal
	LastCmdTs string  `json:"last_cmd_ts,omitempty"`
	Input     string  `json:"input,omitempty"`
	Username  string  `json:"username,omitempty"`
	Format    string  `json:"format,omitempty"`
	Speed     float64 `json:"speed,omitempty"`
	ReadOnly  bool    `json:"readonly,omitempty"`
	Addr      string  `json:"addr,omitempty"`
}

func (job *replayJob) Type() JobType {
	return Replay
}

func (job *replayJob) MarshalJSON() ([]byte, error) {
	job4Marshal := job.getJob4Marshal()
	job4Marshal.Type = "replay"
	r := replayJob4Marshal{
		job4Marshal: *job4Marshal,
		LastCmdTs:   job.lastCmdTs.Format(time.RFC3339Nano),
		Input:       ast.RedactURL(job.cfg.Input),
		Username:    job.cfg.Username,
		Speed:       job.cfg.Speed,
		ReadOnly:    job.cfg.ReadOnly,
		Format:      job.cfg.Format,
		Addr:        job.cfg.Addr,
	}
	return json.Marshal(r)
}

func (job *replayJob) String() string {
	b, err := json.Marshal(job)
	if err != nil {
		return ""
	}
	return hack.String(b)
}
