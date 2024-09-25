// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"crypto/tls"
	"encoding/json"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/id"
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

func NewJobManager(lg *zap.Logger, cfg *config.Config, certMgr CertManager, idMgr *id.IDManager, hsHandler backend.HandshakeHandler) *jobManager {
	return &jobManager{
		lg:          lg,
		capture:     capture.NewCapture(lg.Named("capture")),
		replay:      replay.NewReplay(lg.Named("replay"), idMgr),
		hsHandler:   hsHandler,
		cfg:         cfg,
		certManager: certMgr,
	}
}

func (jm *jobManager) updateProgress() {
	if len(jm.jobHistory) == 0 {
		return
	}
	job := jm.jobHistory[len(jm.jobHistory)-1]
	if job.IsRunning() {
		switch job.Type() {
		case Capture:
			progress, endTime, err := jm.capture.Progress()
			job.SetProgress(progress, endTime, err)
		case Replay:
			progress, endTime, err := jm.replay.Progress()
			job.SetProgress(progress, endTime, err)
		}
	}
}

func (jm *jobManager) runningJob() Job {
	if len(jm.jobHistory) == 0 {
		return nil
	}
	jm.updateProgress()
	job := jm.jobHistory[len(jm.jobHistory)-1]
	if job.IsRunning() {
		return job
	}
	return nil
}

func (jm *jobManager) StartCapture(cfg capture.CaptureConfig) error {
	running := jm.runningJob()
	if running != nil {
		return errors.Errorf("a job is running: %s", running.String())
	}
	if err := jm.capture.Start(cfg); err != nil {
		jm.lg.Warn("start capture failed", zap.Error(err))
		return errors.Wrapf(err, "start capture failed")
	}
	newJob := &captureJob{
		job: job{
			startTime: time.Now(),
		},
		cfg: cfg,
	}
	jm.lg.Info("start capture", zap.String("job", newJob.String()))
	jm.jobHistory = append(jm.jobHistory, newJob)
	return nil
}

func (jm *jobManager) StartReplay(cfg replay.ReplayConfig) error {
	running := jm.runningJob()
	if running != nil {
		return errors.Errorf("a job is running: %s", running.String())
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
		jm.lg.Warn("start replay failed", zap.Error(err))
		return errors.Wrapf(err, "start replay failed")
	}
	newJob := &replayJob{
		job: job{
			startTime: time.Now(),
		},
		cfg: cfg,
	}
	jm.lg.Info("start replay", zap.String("job", newJob.String()))
	jm.jobHistory = append(jm.jobHistory, newJob)
	return nil
}

func (jm *jobManager) GetCapture() capture.Capture {
	return jm.capture
}

func (jm *jobManager) Jobs() string {
	jm.updateProgress()
	b, err := json.MarshalIndent(jm.jobHistory, "", "  ")
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
	jm.updateProgress()
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
