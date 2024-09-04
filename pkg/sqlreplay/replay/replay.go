// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"crypto/tls"

	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
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
	Filename string
	Username string
	Password string
}

type connWrapper struct {
	cmdCh       chan *cmd.Command
	exceptionCh chan Exception
	conn        Conn
}

type connCreator func(connID uint64, cmdCh chan *cmd.Command, exceptionCh chan Exception) Conn

type replay struct {
	cfg         ReplayConfig
	conns       map[uint64]*connWrapper
	connCreator connCreator
	lg          *zap.Logger
}

func NewReplay(lg *zap.Logger, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) *replay {
	r := &replay{
		conns: make(map[uint64]*connWrapper),
		lg:    lg,
	}
	r.connCreator = func(connID uint64, cmdCh chan *cmd.Command, exceptionCh chan Exception) Conn {
		lg = r.lg.With(zap.Uint64("connID", connID))
		return newConn(lg, r.cfg.Username, r.cfg.Password, backendTLSConfig, hsHandler, connID, bcConfig, cmdCh, exceptionCh)
	}
	return r
}

func (r *replay) Start(cfg ReplayConfig) error {
	return nil
}

func (r *replay) executeCmd(ctx context.Context, command *cmd.Command) {
	wrapper, ok := r.conns[command.ConnID]
	if !ok {
		cmdCh, exceptionCh := make(chan *cmd.Command, 1), make(chan Exception, 3)
		wrapper = &connWrapper{
			conn:        r.connCreator(command.ConnID, cmdCh, exceptionCh),
			cmdCh:       cmdCh,
			exceptionCh: exceptionCh,
		}
		r.conns[command.ConnID] = wrapper
		wrapper.conn.Run(ctx)
	}
	if wrapper == nil {
		return
	}

	// drain exceptions
	drained := false
	for !drained {
		select {
		case <-ctx.Done():
			return
		case e := <-wrapper.exceptionCh:
			if e.Critical() {
				// Keep the disconnected connections in the map to reject subsequent commands with the same connID,
				// but release memory as much as possible.
				wrapper.conn.Close()
				r.conns[command.ConnID] = nil
				return
			}
			// TODO: report the exception
		default:
			drained = true
		}
	}

	select {
	case wrapper.cmdCh <- command:
	default: // avoid block due to bug
	}
}

func (r *replay) Stop(err error) {
}

func (r *replay) Close() {
}
