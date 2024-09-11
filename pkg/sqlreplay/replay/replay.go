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
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
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
	Start(cfg ReplayConfig) error
	// Stop stops the replay
	Stop(err error)
	// Close closes the replay
	Close()
}

type ReplayConfig struct {
	Input    string
	Username string
	Password string
	Speed    float64
	reader   cmd.LineReader
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
	conns            map[uint64]conn.Conn
	exceptionCh      chan conn.Exception
	closeCh          chan uint64
	wg               waitgroup.WaitGroup
	cancel           context.CancelFunc
	connCreator      conn.ConnCreator
	report           report.Report
	err              error
	backendTLSConfig *tls.Config
	lg               *zap.Logger
	connCount        int
}

func NewReplay(lg *zap.Logger, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) *replay {
	r := &replay{
		conns:       make(map[uint64]conn.Conn),
		lg:          lg,
		exceptionCh: make(chan conn.Exception, maxPendingExceptions),
		closeCh:     make(chan uint64, maxPendingExceptions),
	}
	r.connCreator = func(connID uint64) conn.Conn {
		return conn.NewConn(lg, r.cfg.Username, r.cfg.Password, backendTLSConfig, hsHandler, connID, bcConfig, r.exceptionCh, r.closeCh)
	}
	backendConnCreator := func() conn.BackendConn {
		// TODO: allocate connection ID.
		return conn.NewBackendConn(lg.Named("be"), 1, hsHandler, bcConfig, backendTLSConfig, r.cfg.Username, r.cfg.Password)
	}
	r.report = report.NewReport(lg.Named("report"), r.exceptionCh, backendConnCreator)
	return r
}

func (r *replay) Start(cfg ReplayConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()
	r.cfg = cfg
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
	r.wg.RunWithRecover(func() {
		r.readCloseCh(childCtx)
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
	var captureStartTs, replayStartTs time.Time
	for ctx.Err() == nil {
		command := &cmd.Command{}
		if err := command.Decode(reader); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			r.Stop(err)
			break
		}
		// Replayer always uses the same username. It has no passwords for other users.
		// TODO: clear the session states.
		if command.Type == pnet.ComChangeUser {
			continue
		}
		if captureStartTs.IsZero() {
			// first command
			captureStartTs = command.StartTs
			replayStartTs = time.Now()
		} else {
			expectedInterval := command.StartTs.Sub(replayStartTs)
			if r.cfg.Speed != 1 {
				expectedInterval = time.Duration(float64(expectedInterval) / r.cfg.Speed)
			}
			curInterval := time.Since(replayStartTs)
			if curInterval+time.Microsecond < expectedInterval {
				select {
				case <-ctx.Done():
				case <-time.After(expectedInterval - curInterval):
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		r.executeCmd(ctx, command)
	}
	reader.Close()
}

func (r *replay) executeCmd(ctx context.Context, command *cmd.Command) {
	r.Lock()
	defer r.Unlock()

	conn, ok := r.conns[command.ConnID]
	if !ok {
		conn = r.connCreator(command.ConnID)
		r.conns[command.ConnID] = conn
		r.connCount++
		r.wg.RunWithRecover(func() {
			conn.Run(ctx)
		}, nil, r.lg)
	}
	if conn != nil && !reflect.ValueOf(conn).IsNil() {
		conn.ExecuteCmd(command)
	}
}

func (r *replay) readCloseCh(ctx context.Context) {
	// Drain all close events even if the context is canceled.
	// Otherwise, the connections may block at writing to channels.
	for {
		if ctx.Err() != nil {
			r.Lock()
			connCount := r.connCount
			r.Unlock()
			if connCount <= 0 {
				return
			}
		}
		select {
		case c, ok := <-r.closeCh:
			if !ok {
				return
			}
			// Keep the disconnected connections in the map to reject subsequent commands with the same connID,
			// but release memory as much as possible.
			r.Lock()
			if conn, ok := r.conns[c]; ok && conn != nil && !reflect.ValueOf(conn).IsNil() {
				r.conns[c] = nil
				r.connCount--
			}
			r.Unlock()
		case <-time.After(100 * time.Millisecond):
			// If context is canceled now but no connection exists, it will block forever.
			// Check the context and connCount again.
		}
	}
}

func (r *replay) Stop(err error) {
	r.Lock()
	defer r.Unlock()
	if r.err == nil {
		r.err = err
	}
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
}

func (r *replay) Close() {
	r.Stop(errors.New("shutting down"))
	r.wg.Wait()
}
