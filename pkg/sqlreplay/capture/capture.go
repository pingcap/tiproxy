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
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"go.uber.org/zap"
)

const (
	bufferCap          = 1 << 24           // 16MB
	flushThreshold     = bufferCap * 3 / 4 // 12MB
	maxBuffers         = 10
	maxPendingCommands = 1 << 14 // 16K
)

type Capture interface {
	// Start starts the capture
	Start(cfg CaptureConfig) error
	// Stop stops the capture.
	// err means the error that caused the capture to stop. nil means the capture stopped manually.
	Stop(err error)
	// Capture captures traffic
	Capture(packet []byte, startTime time.Time, connID uint64)
	// Close closes the capture
	Close()
}

type CaptureConfig struct {
	Output             string
	Duration           time.Duration
	bufferCap          int
	flushThreshold     int
	maxBuffers         int
	maxPendingCommands int
}

func checkCaptureConfig(cfg *CaptureConfig) error {
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
	cfg       CaptureConfig
	wg        waitgroup.WaitGroup
	cancel    context.CancelFunc
	cmdCh     chan *cmd.Command
	err       error
	startTime time.Time
	cmdLogger store.Writer
	lg        *zap.Logger
}

func NewCapture(lg *zap.Logger) *capture {
	return &capture{
		lg: lg,
	}
}

func (c *capture) Start(cfg CaptureConfig) error {
	if err := checkCaptureConfig(&cfg); err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()
	if !c.startTime.IsZero() {
		return errors.Errorf("traffic capture is running, start time: %s", c.startTime.String())
	}

	c.stopNoLock(nil)
	// It may be set in tests.
	if c.cmdLogger == nil {
		c.cmdLogger = store.NewWriter(store.WriterCfg{Dir: cfg.Output})
	}
	c.cfg = cfg
	c.startTime = time.Now()
	c.err = nil
	childCtx, cancel := context.WithTimeout(context.Background(), c.cfg.Duration)
	c.cancel = cancel
	bufCh := make(chan *bytes.Buffer, cfg.maxBuffers)
	c.cmdCh = make(chan *cmd.Command, cfg.maxPendingCommands)
	c.wg.RunWithRecover(func() {
		<-childCtx.Done()
		c.Stop(nil)
	}, nil, c.lg)
	c.wg.RunWithRecover(func() {
		c.collectCmds(bufCh)
	}, nil, c.lg)
	c.wg.RunWithRecover(func() {
		c.flushBuffer(bufCh)
	}, nil, c.lg)
	return nil
}

func (c *capture) collectCmds(bufCh chan<- *bytes.Buffer) {
	defer close(bufCh)

	buf := bytes.NewBuffer(make([]byte, 0, c.cfg.bufferCap))
	// Flush all commands even if the context is timeout.
	for command := range c.cmdCh {
		if err := command.Encode(buf); err != nil {
			c.Stop(errors.Wrapf(err, "failed to encode command"))
			return
		}
		if buf.Len() > c.cfg.flushThreshold {
			select {
			case bufCh <- buf:
			default:
				// Don't wait, otherwise the QPS may be affected.
				c.Stop(errors.New("flushing traffic to disk is too slow, buffer is full"))
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
	// Flush all buffers even if the context is timeout.
	for buf := range bufCh {
		// TODO: each write size should be less than MaxSize.
		if err := c.cmdLogger.Write(buf.Bytes()); err != nil {
			c.Stop(errors.Wrapf(err, "failed to flush traffic to disk"))
			return
		}
	}
}

func (c *capture) Capture(packet []byte, startTime time.Time, connID uint64) {
	c.Lock()
	defer c.Unlock()
	// not capturing
	if c.startTime.IsZero() {
		return
	}

	command := cmd.NewCommand(packet, startTime, connID)
	if command == nil {
		return
	}
	// TODO: handle QUIT
	select {
	case c.cmdCh <- command:
	default:
		// Don't wait, otherwise the QPS may be affected.
		c.stopNoLock(errors.New("encoding traffic is too slow, buffer is full"))
	}
}

// stopNoLock must be called after holding a lock.
func (c *capture) stopNoLock(err error) {
	// already stopped
	if c.startTime.IsZero() {
		return
	}
	if err != nil {
		c.lg.Error("stop capture", zap.Error(err))
	}
	c.err = err
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.startTime = time.Time{}
	close(c.cmdCh)
	if err := c.cmdLogger.Close(); err != nil {
		c.lg.Warn("failed to close command logger", zap.Error(err))
	}
	c.cmdLogger = nil
}

func (c *capture) Stop(err error) {
	c.Lock()
	defer c.Unlock()
	c.stopNoLock(err)
}

func (c *capture) Close() {
	c.Stop(errors.New("shutting down"))
	c.wg.Wait()
}
