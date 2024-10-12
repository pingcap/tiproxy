// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"bytes"
	"context"
	"os"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

const (
	bufferCap          = 1 << 24           // 16MB
	flushThreshold     = bufferCap * 3 / 4 // 12MB
	maxBuffers         = 10
	maxPendingCommands = 1 << 14 // 16K
)

const (
	statusIdle = iota
	statusRunning
	// capture is stopped but data is writing
	statusStopping
)

type Capture interface {
	// Start starts the capture
	Start(cfg CaptureConfig) error
	// Stop stops the capture.
	// err means the error that caused the capture to stop. nil means the capture stopped manually.
	Stop(err error)
	// InitConn is called when a new connection is created.
	InitConn(startTime time.Time, connID uint64, db string)
	// Capture captures traffic
	Capture(packet []byte, startTime time.Time, connID uint64, initSession func() (string, error))
	// Progress returns the progress of the capture job
	Progress() (float64, time.Time, error)
	// Close closes the capture
	Close()
}

type CaptureConfig struct {
	Output             string
	Duration           time.Duration
	cmdLogger          store.Writer
	bufferCap          int
	flushThreshold     int
	maxBuffers         int
	maxPendingCommands int
}

func (cfg *CaptureConfig) Validate() error {
	if cfg.Output == "" {
		return errors.New("output is required")
	}
	st, err := os.Stat(cfg.Output)
	if err == nil {
		if !st.IsDir() {
			return errors.New("output should be a directory")
		}
	} else if os.IsNotExist(err) {
		err = os.MkdirAll(cfg.Output, 0755)
	}
	if err != nil {
		return err
	}
	if cfg.Duration == 0 {
		return errors.New("duration is required")
	}
	if cfg.bufferCap == 0 {
		cfg.bufferCap = bufferCap
	}
	if cfg.flushThreshold == 0 {
		cfg.flushThreshold = flushThreshold
	}
	if cfg.maxBuffers == 0 {
		cfg.maxBuffers = maxBuffers
	}
	if cfg.maxPendingCommands == 0 {
		cfg.maxPendingCommands = maxPendingCommands
	}
	return nil
}

var _ Capture = (*capture)(nil)

type capture struct {
	sync.Mutex
	cfg          CaptureConfig
	conns        map[uint64]struct{}
	wg           waitgroup.WaitGroup
	cancel       context.CancelFunc
	cmdCh        chan *cmd.Command
	err          error
	startTime    time.Time
	endTime      time.Time
	progress     float64
	capturedCmds uint64
	filteredCmds uint64
	status       int
	lg           *zap.Logger
}

func NewCapture(lg *zap.Logger) *capture {
	return &capture{
		lg: lg,
	}
}

func (c *capture) Start(cfg CaptureConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()
	if c.status != statusIdle {
		return errors.Errorf("traffic capture is running, start time: %s", c.startTime.String())
	}
	c.cfg = cfg
	c.startTime = time.Now()
	c.endTime = time.Time{}
	c.progress = 0
	c.capturedCmds = 0
	c.filteredCmds = 0
	c.status = statusRunning
	c.err = nil
	c.conns = make(map[uint64]struct{})
	childCtx, cancel := context.WithTimeout(context.Background(), c.cfg.Duration)
	c.cancel = cancel
	bufCh := make(chan *bytes.Buffer, cfg.maxBuffers)
	c.cmdCh = make(chan *cmd.Command, cfg.maxPendingCommands)
	c.wg.RunWithRecover(func() {
		c.run(childCtx, bufCh)
	}, nil, c.lg)
	return nil
}

func (c *capture) run(ctx context.Context, bufCh chan *bytes.Buffer) {
	var wg waitgroup.WaitGroup
	wg.RunWithRecover(func() {
		c.collectCmds(bufCh)
	}, nil, c.lg)
	wg.RunWithRecover(func() {
		c.flushBuffer(bufCh)
	}, nil, c.lg)
	<-ctx.Done()
	c.stop(nil)
	wg.Wait()

	c.Lock()
	defer c.Unlock()
	c.status = statusIdle
	c.endTime = time.Now()
	duration := c.endTime.Sub(c.startTime)
	fields := []zap.Field{
		zap.Time("start_time", c.startTime),
		zap.Time("end_time", c.endTime),
		zap.Duration("duration", duration),
		zap.Uint64("captured_cmds", c.capturedCmds),
	}
	if c.err != nil {
		if c.cfg.Duration > 0 {
			c.progress = float64(duration) / float64(c.cfg.Duration)
			if c.progress > 1 {
				c.progress = 1
			}
		}
		fields = append(fields, zap.Error(c.err))
		c.lg.Error("capture failed", fields...)
	} else {
		c.progress = 1
		c.lg.Info("capture finished", fields...)
	}
}

func (c *capture) collectCmds(bufCh chan<- *bytes.Buffer) {
	defer close(bufCh)

	buf := bytes.NewBuffer(make([]byte, 0, c.cfg.bufferCap))
	// Flush all commands even if the context is timeout.
	for command := range c.cmdCh {
		if err := command.Encode(buf); err != nil {
			c.stop(errors.Wrapf(err, "failed to encode command"))
			continue
		}
		c.Lock()
		c.capturedCmds++
		c.Unlock()
		if buf.Len() > c.cfg.flushThreshold {
			select {
			case bufCh <- buf:
			default:
				// Don't wait, otherwise the QPS may be affected.
				c.stop(errors.New("flushing traffic to disk is too slow, buffer is full"))
				return
			}
			buf = bytes.NewBuffer(make([]byte, 0, c.cfg.bufferCap))
		}
	}

	if buf.Len() > 0 {
		bufCh <- buf
	}
}

func (c *capture) flushBuffer(bufCh <-chan *bytes.Buffer) {
	// cfg.cmdLogger is set in tests
	cmdLogger := c.cfg.cmdLogger
	if cmdLogger == nil {
		cmdLogger = store.NewWriter(store.WriterCfg{Dir: c.cfg.Output})
	}
	// Flush all buffers even if the context is timeout.
	for buf := range bufCh {
		// TODO: each write size should be less than MaxSize.
		if err := cmdLogger.Write(buf.Bytes()); err != nil {
			c.stop(errors.Wrapf(err, "failed to flush traffic to disk"))
			break
		}
	}
	if err := cmdLogger.Close(); err != nil {
		c.lg.Warn("failed to close command logger", zap.Error(err))
	}

	c.Lock()
	startTime := c.startTime
	capturedCmds := c.capturedCmds
	c.Unlock()
	// Write meta outside of the lock to avoid affecting QPS.
	c.writeMeta(time.Since(startTime), capturedCmds)
}

func (c *capture) InitConn(startTime time.Time, connID uint64, db string) {
	c.Lock()
	defer c.Unlock()
	if c.status != statusRunning {
		return
	}
	if db != "" {
		packet := make([]byte, 0, len(db)+1)
		packet = append(packet, pnet.ComInitDB.Byte())
		packet = append(packet, hack.Slice(db)...)
		command := cmd.NewCommand(packet, startTime, connID)
		if command == nil {
			return
		}
		c.putCommand(command)
	}
	c.conns[connID] = struct{}{}
}

func (c *capture) Capture(packet []byte, startTime time.Time, connID uint64, initSession func() (string, error)) {
	c.Lock()
	if c.status != statusRunning {
		c.Unlock()
		return
	}
	_, inited := c.conns[connID]
	c.Unlock()

	// If this is the first command for this connection, record a `set session_states` statement.
	if !inited {
		// Maybe it's quitting, no need to init session.
		if initSession == nil || len(packet) == 0 || packet[0] == pnet.ComQuit.Byte() {
			return
		}
		// initSession is slow, do not call it in the lock.
		sql, err := initSession()
		if err != nil {
			// Maybe the connection is in transaction or closing.
			c.lg.Debug("failed to init session", zap.Uint64("connID", connID), zap.Error(err))
			return
		}
		initPacket := make([]byte, 0, len(sql)+1)
		initPacket = append(initPacket, pnet.ComQuery.Byte())
		initPacket = append(initPacket, hack.Slice(sql)...)
		command := cmd.NewCommand(initPacket, startTime, connID)
		c.Lock()
		if c.putCommand(command) {
			c.conns[connID] = struct{}{}
		}
		c.Unlock()
	}

	command := cmd.NewCommand(packet, startTime, connID)
	if command == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.putCommand(command)
}

func (c *capture) putCommand(command *cmd.Command) bool {
	if c.status != statusRunning {
		return false
	}
	switch command.Type {
	case pnet.ComQuit:
		if _, ok := c.conns[command.ConnID]; ok {
			delete(c.conns, command.ConnID)
		} else {
			// Duplicated quit, ignore it.
			return false
		}
	case pnet.ComChangeUser:
		// COM_CHANGE_USER sends auth data, change it to COM_RESET_CONNECTION.
		command.Type = pnet.ComResetConnection
		command.Payload = []byte{pnet.ComResetConnection.Byte()}
	case pnet.ComQuery:
		// Avoid password leakage.
		if IsSensitiveSQL(hack.String(command.Payload[1:])) {
			c.filteredCmds++
			return false
		}
	}
	select {
	case c.cmdCh <- command:
		return true
	default:
		// Don't wait, otherwise the QPS may be affected.
		c.stopNoLock(errors.New("encoding traffic is too slow, buffer is full"))
		return false
	}
}

func (c *capture) writeMeta(duration time.Duration, cmds uint64) {
	meta := store.Meta{Duration: duration, Cmds: cmds}
	if err := meta.Write(c.cfg.Output); err != nil {
		c.lg.Error("failed to write meta", zap.Error(err))
	}
}

func (c *capture) Progress() (float64, time.Time, error) {
	c.Lock()
	defer c.Unlock()
	if c.status == statusIdle || c.cfg.Duration == 0 {
		return c.progress, c.endTime, c.err
	}
	progress := float64(time.Since(c.startTime)) / float64(c.cfg.Duration)
	if progress > 1 {
		progress = 1
	}
	return progress, c.endTime, c.err
}

// stopNoLock must be called after holding a lock.
func (c *capture) stopNoLock(err error) {
	if c.status != statusRunning {
		return
	}
	c.status = statusStopping
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	close(c.cmdCh)
	if c.err == nil {
		c.err = err
	}
	c.conns = map[uint64]struct{}{}
}

func (c *capture) stop(err error) {
	c.Lock()
	c.stopNoLock(err)
	c.Unlock()
}

func (c *capture) Stop(err error) {
	c.stop(err)
	c.wg.Wait()
}

func (c *capture) Close() {
	c.Stop(errors.New("shutting down"))
}
