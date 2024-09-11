// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"crypto/tls"
	"encoding/json"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

type CertManager interface {
	SQLTLS() *tls.Config
}

type JobManager interface {
	StartCapture(capture.CaptureConfig) error
	StartReplay(replay.ReplayConfig) error
	GetCapture() capture.Capture
	Stop() string
	Jobs() string
	Close()
}

var _ JobManager = (*jobManager)(nil)

type jobManager struct {
	jobHistory  []Job
	capture     capture.Capture
	replay      replay.Replay
	hsHandler   backend.HandshakeHandler
	certManager CertManager
	cfg         *config.Config
	lg          *zap.Logger
}

func NewJobManager(lg *zap.Logger, cfg *config.Config, certMgr CertManager, hsHandler backend.HandshakeHandler) *jobManager {
	return &jobManager{
		lg:          lg,
		capture:     capture.NewCapture(lg.Named("capture")),
		replay:      replay.NewReplay(lg.Named("replay")),
		hsHandler:   hsHandler,
		cfg:         cfg,
		certManager: certMgr,
	}
}

func (jm *jobManager) runningJob() Job {
	if len(jm.jobHistory) == 0 {
		return nil
	}
	job := jm.jobHistory[len(jm.jobHistory)-1]
	if job.IsRunning() {
		switch job.Type() {
		case Capture:
			progress, err := jm.capture.Progress()
			job.SetProgress(progress, err)
		case Replay:
			progress, err := jm.replay.Progress()
			job.SetProgress(progress, err)
		}
		if job.IsRunning() {
			return job
		}
	}
	return nil
}

func (jm *jobManager) StartCapture(cfg capture.CaptureConfig) error {
	running := jm.runningJob()
	if running != nil {
		return errors.Errorf("a job is running: %s", running.String())
	}
	newJob := &captureJob{
		job: job{
			StartTime: time.Now(),
			Duration:  cfg.Duration,
		},
	}
	err := jm.capture.Start(cfg)
	if err != nil {
		newJob.SetProgress(0, err)
	}
	jm.jobHistory = append(jm.jobHistory, newJob)
	return errors.Wrapf(err, "start capture failed")
}

func (jm *jobManager) StartReplay(cfg replay.ReplayConfig) error {
	running := jm.runningJob()
	if running != nil {
		return errors.Errorf("a job is running: %s", running.String())
	}
	newJob := &replayJob{
		job: job{
			StartTime: time.Now(),
		},
	}
	// TODO: support update configs online
	err := jm.replay.Start(cfg, jm.certManager.SQLTLS(), jm.hsHandler, &backend.BCConfig{
		ProxyProtocol:      jm.cfg.Proxy.ProxyProtocol != "",
		RequireBackendTLS:  jm.cfg.Security.RequireBackendTLS,
		HealthyKeepAlive:   jm.cfg.Proxy.BackendHealthyKeepalive,
		UnhealthyKeepAlive: jm.cfg.Proxy.BackendUnhealthyKeepalive,
		ConnBufferSize:     jm.cfg.Proxy.ConnBufferSize,
	})
	if err != nil {
		newJob.SetProgress(0, err)
	}
	jm.jobHistory = append(jm.jobHistory, newJob)
	return errors.Wrapf(err, "start replay failed")
}

func (jm *jobManager) GetCapture() capture.Capture {
	return jm.capture
}

func (jm *jobManager) Jobs() string {
	b, err := json.Marshal(jm.jobHistory)
	if err != nil {
		return err.Error()
	}
	return hack.String(b)
}

func (jm *jobManager) Stop() string {
	job := jm.runningJob()
	if job == nil {
		return "no job running"
	}
	switch job.Type() {
	case Capture:
		jm.capture.Stop(errors.Errorf("manually stopped"))
	case Replay:
		jm.replay.Stop(errors.Errorf("manually stopped"))
	}
	job.SetProgress(0, errors.Errorf("manually stopped"))
	return "stopped: " + job.String()
}

func (jm *jobManager) Close() {
	if jm.capture != nil {
		jm.capture.Close()
	}
	if jm.replay != nil {
		jm.replay.Close()
	}
}
