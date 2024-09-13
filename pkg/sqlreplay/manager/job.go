// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"encoding/json"
	"time"

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
	SetProgress(progress float64, err error)
	IsRunning() bool
}

type job struct {
	Status    string
	StartTime time.Time
	Duration  time.Duration
	Progress  float64
	// TODO: error can not be marshaled.
	Error error
}

func (job *job) IsRunning() bool {
	return job.Error == nil && job.Progress < 1
}

// TODO: refine the output
func (job *job) String() string {
	b, err := json.Marshal(job)
	if err != nil {
		return err.Error()
	}
	return hack.String(b)
}

func (job *job) SetProgress(progress float64, err error) {
	if progress > job.Progress {
		job.Progress = progress
	}
	job.Error = err
}

var _ Job = (*captureJob)(nil)

type captureJob struct {
	job
}

func (job *captureJob) Type() jobType {
	return Capture
}

var _ Job = (*replayJob)(nil)

type replayJob struct {
	job
}

func (job *replayJob) Type() jobType {
	return Replay
}
