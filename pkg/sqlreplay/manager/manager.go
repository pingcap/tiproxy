// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"crypto/tls"
	"encoding/json"
	"sync"
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

const (
	maxJobHistoryCount = 10
	connectTimeout     = 60 * time.Second
	dialTimeout        = 5 * time.Second
)

type CancelConfig struct {
	Type     JobType
	Graceful bool
}

type CertManager interface {
	SQLTLS() *tls.Config
}

type JobManager interface {
	StartCapture(capture.CaptureConfig) error
	StartReplay(replay.ReplayConfig) error
	GetCapture() capture.Capture
	Wait()
	Stop(CancelConfig) string
	Jobs() string
	Close()
}

var _ JobManager = (*jobManager)(nil)

type jobManager struct {
	mu sync.Mutex

	jobHistory         []Job
	capture            capture.Capture
	replay             replay.Replay
	hsHandler          backend.HandshakeHandler
	certManager        CertManager
	cfg                *config.Config
	lg                 *zap.Logger
	isStandalonePlayer bool
}

func NewJobManager(lg *zap.Logger, cfg *config.Config, certMgr CertManager, idMgr *id.IDManager, hsHandler backend.HandshakeHandler, isStandalonePlayer bool) *jobManager {
	return &jobManager{
		lg:                 lg,
		capture:            capture.NewCapture(lg.Named("capture")),
		replay:             replay.NewReplay(lg.Named("replay"), idMgr),
		hsHandler:          hsHandler,
		cfg:                cfg,
		certManager:        certMgr,
		isStandalonePlayer: isStandalonePlayer,
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
			progress, endTime, done, err := jm.capture.Progress()
			job.SetProgress(progress, endTime, done, err)
		case Replay:
			progress, endTime, curCmdTs, curCmdEndTs, done, err := jm.replay.Progress()
			job.SetProgress(progress, endTime, done, err)
			job.(*replayJob).lastCmdTs = curCmdTs
			job.(*replayJob).lastCmdEndTs = curCmdEndTs
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
	jm.mu.Lock()
	defer jm.mu.Unlock()

	running := jm.runningJob()
	if running != nil {
		return errors.Errorf("a job is running: %s", running.String())
	}
	newJob := &captureJob{
		job: job{
			// cfg.StartTime may act as the job ID in a TiProxy cluster.
			startTime: cfg.StartTime,
		},
		cfg: cfg,
	}
	if err := jm.capture.Start(cfg); err != nil {
		jm.lg.Warn("start capture failed", zap.String("job", newJob.String()), zap.Error(err))
		return errors.Wrapf(err, "start capture failed")
	}
	jm.lg.Info("start capture", zap.String("job", newJob.String()))
	jm.addToHistory(newJob)
	return nil
}

func (jm *jobManager) StartReplay(cfg replay.ReplayConfig) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	running := jm.runningJob()
	if running != nil {
		return errors.Errorf("a job is running: %s", running.String())
	}

	if len(cfg.Addr) > 0 {
		if !jm.isStandalonePlayer {
			return errors.Errorf("Addr is not allowed in replay config in a TiProxy node")
		}
		// override the hsHandler
		jm.hsHandler = backend.NewStaticHandshakeHandler(cfg.Addr)
	}

	newJob := &replayJob{
		job: job{
			// cfg.StartTime may act as the job ID in a TiProxy cluster.
			startTime: cfg.StartTime,
		},
		cfg: cfg,
	}
	// TODO: support update configs online
	err := jm.replay.Start(cfg, jm.certManager.SQLTLS(), jm.hsHandler, &backend.BCConfig{
		ProxyProtocol:      jm.cfg.Proxy.ProxyProtocol != "",
		RequireBackendTLS:  jm.cfg.Security.RequireBackendTLS,
		HealthyKeepAlive:   jm.cfg.Proxy.BackendHealthyKeepalive,
		UnhealthyKeepAlive: jm.cfg.Proxy.BackendUnhealthyKeepalive,
		ConnBufferSize:     jm.cfg.Proxy.ConnBufferSize,
		DialTimeout:        dialTimeout,
		ConnectTimeout:     connectTimeout,
	})
	if err != nil {
		jm.lg.Warn("start replay failed", zap.String("job", newJob.String()), zap.Error(err))
		return errors.Wrapf(err, "start replay failed")
	}
	// jm.lg.Info("start replay", zap.String("job", newJob.String()))
	jm.addToHistory(newJob)
	return nil
}

func (jm *jobManager) addToHistory(newJob Job) {
	if len(jm.jobHistory) >= maxJobHistoryCount {
		copy(jm.jobHistory, jm.jobHistory[1:])
		jm.jobHistory[len(jm.jobHistory)-1] = newJob
	} else {
		jm.jobHistory = append(jm.jobHistory, newJob)
	}
}

func (jm *jobManager) GetCapture() capture.Capture {
	return jm.capture
}

func (jm *jobManager) Jobs() string {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	jm.updateProgress()
	b, err := json.MarshalIndent(jm.jobHistory, "", "  ")
	if err != nil {
		return err.Error()
	}
	return hack.String(b)
}

// Wait waits for the running job to finish.
// As `Wait` is a blocking call, it'll not acquire the jobManager lock. For now it's only used in standalone player mode.
func (jm *jobManager) Wait() {
	job := jm.runningJob()
	if job == nil {
		return
	}
	switch job.Type() {
	case Capture:
		jm.capture.Wait()
	case Replay:
		jm.replay.Wait()
	}
}

func (jm *jobManager) Stop(cfg CancelConfig) string {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job := jm.runningJob()
	if job == nil {
		return "no job running"
	}
	if job.Type()&cfg.Type == 0 {
		return "no privilege to stop the job"
	}
	switch job.Type() {
	case Capture:
		jm.capture.Stop(errors.Errorf("manually stopped"))
	case Replay:
		jm.replay.Stop(errors.Errorf("manually stopped, graceful: %v", cfg.Graceful), cfg.Graceful)
	}
	jm.updateProgress()
	return "stopped: " + job.String()
}

func (jm *jobManager) Close() {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if jm.capture != nil {
		jm.capture.Close()
	}
	if jm.replay != nil {
		jm.replay.Close()
	}
}
