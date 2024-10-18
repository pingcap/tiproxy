// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"crypto/tls"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/report"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"go.uber.org/zap"
)

const (
	maxPendingExceptions = 1024 // pending exceptions for all connections
	minSpeed             = 0.1
	maxSpeed             = 10.0
)

type Replay interface {
	// Start starts the replay
	Start(cfg ReplayConfig, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) error
	// Stop stops the replay
	Stop(err error)
	// Progress returns the progress of the replay job
	Progress() (float64, time.Time, error)
	// Close closes the replay
	Close()
}

type ReplayConfig struct {
	Input    string
	Username string
	Password string
	Speed    float64
	// the following fields are for testing
	reader      cmd.LineReader
	report      report.Report
	connCreator conn.ConnCreator
}

func (cfg *ReplayConfig) Validate() error {
	if cfg.Input == "" {
		return errors.New("input is required")
	}
	st, err := os.Stat(cfg.Input)
	if err == nil {
		if !st.IsDir() {
			return errors.New("output should be a directory")
		}
	} else {
		return errors.WithStack(err)
	}
	if cfg.Username == "" {
		return errors.New("username is required")
	}
	if cfg.Speed == 0 {
		cfg.Speed = 1
	} else if cfg.Speed < minSpeed || cfg.Speed > maxSpeed {
		return errors.Errorf("speed should be between %f and %f", minSpeed, maxSpeed)
	}
	return nil
}

type replay struct {
	sync.Mutex
	cfg              ReplayConfig
	meta             store.Meta
	replayStats      conn.ReplayStats
	idMgr            *id.IDManager
	exceptionCh      chan conn.Exception
	closeCh          chan uint64
	wg               waitgroup.WaitGroup
	cancel           context.CancelFunc
	connCreator      conn.ConnCreator
	report           report.Report
	err              error
	startTime        time.Time
	endTime          time.Time
	progress         float64
	decodedCmds      uint64
	backendTLSConfig *tls.Config
	lg               *zap.Logger
}

func NewReplay(lg *zap.Logger, idMgr *id.IDManager) *replay {
	return &replay{
		lg:    lg,
		idMgr: idMgr,
	}
}

func (r *replay) Start(cfg ReplayConfig, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()
	r.cfg = cfg
	r.meta = *r.readMeta()
	r.startTime = time.Now()
	r.endTime = time.Time{}
	r.progress = 0
	r.err = nil
	r.replayStats.Reset()
	r.exceptionCh = make(chan conn.Exception, maxPendingExceptions)
	r.closeCh = make(chan uint64, maxPendingExceptions)
	hsHandler = NewHandshakeHandler(hsHandler)
	r.connCreator = cfg.connCreator
	if r.connCreator == nil {
		r.connCreator = func(connID uint64) conn.Conn {
			return conn.NewConn(r.lg.Named("conn"), r.cfg.Username, r.cfg.Password, backendTLSConfig, hsHandler, r.idMgr,
				connID, bcConfig, r.exceptionCh, r.closeCh, &r.replayStats)
		}
	}
	r.report = cfg.report
	if r.report == nil {
		backendConnCreator := func() conn.BackendConn {
			return conn.NewBackendConn(r.lg.Named("be"), r.idMgr.NewID(), hsHandler, bcConfig, backendTLSConfig, r.cfg.Username, r.cfg.Password)
		}
		r.report = report.NewReport(r.lg.Named("report"), r.exceptionCh, backendConnCreator)
	}

	childCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	if err := r.report.Start(childCtx, report.ReportConfig{
		TlsConfig: r.backendTLSConfig,
	}); err != nil {
		return err
	}
	r.wg.RunWithRecover(func() {
		r.readCommands(childCtx)
	}, nil, r.lg)
	return nil
}

func (r *replay) readCommands(ctx context.Context) {
	// cfg.reader is set in tests
	reader := r.cfg.reader
	if reader == nil {
		reader = store.NewLoader(r.lg.Named("loader"), store.LoaderCfg{
			Dir: r.cfg.Input,
		})
	}
	defer reader.Close()

	var captureStartTs, replayStartTs time.Time
	conns := make(map[uint64]conn.Conn) // both alive and dead connections
	connCount := 0                      // alive connection count
	var err error
	for ctx.Err() == nil {
		for hasCloseEvent := true; hasCloseEvent; {
			select {
			case id := <-r.closeCh:
				r.closeConn(id, conns, &connCount)
			default:
				hasCloseEvent = false
			}
		}

		command := &cmd.Command{}
		if err = command.Decode(reader); err != nil {
			if errors.Is(err, io.EOF) {
				r.lg.Info("replay reads EOF", zap.String("reader", reader.String()))
				err = nil
			}
			break
		}
		if captureStartTs.IsZero() {
			// first command
			captureStartTs = command.StartTs
			replayStartTs = time.Now()
		} else {
			pendingCmds := r.replayStats.PendingCmds.Load()
			if pendingCmds > 1<<20 {
				err = errors.Errorf("too many pending commands, quit replay, pending_cmds: %d", pendingCmds)
				r.lg.Error("too many pending commands, quit replay", zap.Int64("pending_cmds", pendingCmds))
				break
			}
			extraWait := time.Duration(pendingCmds) * time.Microsecond
			expectedInterval := command.StartTs.Sub(captureStartTs)
			if r.cfg.Speed != 1 {
				expectedInterval = time.Duration(float64(expectedInterval) / r.cfg.Speed)
			}
			expectedInterval += extraWait
			curInterval := time.Since(replayStartTs)
			if curInterval+time.Microsecond < expectedInterval {
				select {
				case <-ctx.Done():
				case <-time.After(expectedInterval - curInterval):
				}
			}
		}
		if ctx.Err() == nil {
			r.executeCmd(ctx, command, conns, &connCount)
		}
	}

	// Make the connections stop.
	r.Lock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.Unlock()

	// Drain all close events even if the context is canceled.
	// Otherwise, the connections may block at writing to channels.
	for connCount > 0 {
		id := <-r.closeCh
		r.closeConn(id, conns, &connCount)
	}

	r.stop(err)
}

func (r *replay) executeCmd(ctx context.Context, command *cmd.Command, conns map[uint64]conn.Conn, connCount *int) {
	conn, ok := conns[command.ConnID]
	if !ok {
		conn = r.connCreator(command.ConnID)
		conns[command.ConnID] = conn
		*connCount++
		r.wg.RunWithRecover(func() {
			conn.Run(ctx)
		}, nil, r.lg)
	}
	if conn != nil && !reflect.ValueOf(conn).IsNil() {
		conn.ExecuteCmd(command)
	}

	r.Lock()
	r.decodedCmds++
	r.Unlock()
}

func (r *replay) closeConn(connID uint64, conns map[uint64]conn.Conn, connCount *int) {
	// Keep the disconnected connections in the map to reject subsequent commands with the same connID,
	// but release memory as much as possible.
	if conn, ok := conns[connID]; ok && conn != nil && !reflect.ValueOf(conn).IsNil() {
		conns[connID] = nil
		*connCount--
	}
}

func (r *replay) Progress() (float64, time.Time, error) {
	pendingCmds := r.replayStats.PendingCmds.Load()
	r.Lock()
	defer r.Unlock()
	if r.meta.Cmds > 0 {
		r.progress = float64(r.decodedCmds-uint64(pendingCmds)) / float64(r.meta.Cmds)
	}
	return r.progress, r.endTime, r.err
}

func (r *replay) readMeta() *store.Meta {
	m := new(store.Meta)
	if err := m.Read(r.cfg.Input); err != nil {
		r.lg.Error("read meta failed", zap.Error(err))
	}
	return m
}

func (r *replay) stop(err error) {
	r.Lock()
	defer r.Unlock()

	r.endTime = time.Now()
	// decodedCmds - pendingCmds may be greater than replayedCmds because if a connection is closed unexpectedly,
	// the pending commands of that connection are discarded. We calculate the progress based on decodedCmds - pendingCmds.
	replayedCmds := r.replayStats.ReplayedCmds.Load()
	pendingCmds := r.replayStats.PendingCmds.Load()
	fields := []zap.Field{
		zap.Time("start_time", r.startTime),
		zap.Time("end_time", r.endTime),
		zap.Uint64("decoded_cmds", r.decodedCmds),
		zap.Uint64("replayed_cmds", replayedCmds),
		zap.Int64("pending_cmds", pendingCmds),
	}
	if r.meta.Cmds > 0 {
		r.progress = float64(r.decodedCmds-uint64(pendingCmds)) / float64(r.meta.Cmds)
		fields = append(fields, zap.Uint64("captured_cmds", r.meta.Cmds))
		fields = append(fields, zap.Float64("progress", r.progress))
	}
	if r.err == nil {
		r.err = err
	}
	if r.err != nil {
		fields = append(fields, zap.Error(r.err))
		r.lg.Error("replay failed", fields...)
	} else {
		r.lg.Info("replay finished", fields...)
	}
	r.startTime = time.Time{}
}

func (r *replay) Stop(err error) {
	r.Lock()
	// already stopped
	if r.startTime.IsZero() {
		r.Unlock()
		return
	}
	r.err = err
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.Unlock()
	r.wg.Wait()
}

func (r *replay) Close() {
	r.Stop(errors.New("shutting down"))
}
