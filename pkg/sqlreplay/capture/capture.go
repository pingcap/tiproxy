// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/pingcap/tiproxy/pkg/util/lex"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
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
	// Wait for the job done.
	Wait()
	// Stop stops the capture.
	// err means the error that caused the capture to stop. nil means the capture stopped manually.
	Stop(err error)
	// InitConn is called when a new connection is created.
	InitConn(startTime time.Time, connID uint64, db string)
	// Capture captures traffic
	Capture(packet []byte, startTime time.Time, connID uint64, initSession func() (string, error))
	// Progress returns the progress of the capture job
	Progress() (float64, time.Time, bool, error)
	// Close closes the capture
	Close()
}

type CaptureConfig struct {
	Output           string
	EncryptionMethod string
	KeyFile          string
	// It's specified when executing with the statement `TRAFFIC CAPTURE` so that all TiProxy instances
	// use the same start time and the time acts as the job ID.
	StartTime          time.Time
	Duration           time.Duration
	Compress           bool
	encryptionKey      []byte
	cmdLogger          io.WriteCloser
	bufferCap          int
	flushThreshold     int
	maxBuffers         int
	maxPendingCommands int
}

func (cfg *CaptureConfig) Validate() (storage.ExternalStorage, error) {
	if cfg.Output == "" {
		return nil, errors.New("output is required")
	}
	storage, err := store.NewStorage(cfg.Output)
	if err != nil {
		return storage, err
	}
	if err = store.PreCheckMeta(storage); err != nil {
		return storage, err
	}
	if cfg.Duration == 0 {
		return storage, errors.New("duration is required")
	}
	// Maybe there's a time bias between TiDB and TiProxy, so add one minute.
	now := time.Now()
	if cfg.StartTime.IsZero() {
		return storage, errors.New("start time is not specified")
	} else if now.Add(time.Minute).Before(cfg.StartTime) {
		return storage, errors.New("start time should not be in the future")
	} else if cfg.StartTime.Add(cfg.Duration).Before(now) {
		return storage, errors.New("start time should not be in the past")
	}
	key, err := store.LoadEncryptionKey(cfg.EncryptionMethod, cfg.KeyFile)
	if err != nil {
		return storage, errors.Wrapf(err, "failed to load encryption key")
	}
	cfg.encryptionKey = key
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
	return storage, nil
}

var _ Capture = (*capture)(nil)

type capture struct {
	sync.Mutex
	cfg          CaptureConfig
	conns        map[uint64]struct{}
	wg           waitgroup.WaitGroup
	cancel       context.CancelFunc
	storage      storage.ExternalStorage
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
	storage, err := cfg.Validate()
	if err != nil {
		storage.Close()
		return err
	}

	c.Lock()
	defer c.Unlock()
	if c.status != statusIdle {
		return errors.Errorf("traffic capture is running, start time: %s", c.startTime.String())
	}
	c.cfg = cfg
	c.storage = storage
	c.startTime = cfg.StartTime
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
	if c.storage != nil && !reflect.ValueOf(c.storage).IsNil() {
		c.storage.Close()
		c.storage = nil
	}
}

func (c *capture) collectCmds(bufCh chan<- *bytes.Buffer) {
	defer close(bufCh)

	buf := bytes.NewBuffer(make([]byte, 0, c.cfg.bufferCap))
	encoder := cmd.NewCmdEncoder(cmd.FormatNative)
	// Flush all commands even if the context is timeout.
	for command := range c.cmdCh {
		if err := encoder.Encode(command, buf); err != nil {
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

// Writing commands requires a bytes buffer instead of a simple bufio.Writer,
// so the buffer can not be pushed down to the store package.
func (c *capture) flushBuffer(bufCh <-chan *bytes.Buffer) {
	// cfg.cmdLogger is set in tests
	cmdLogger := c.cfg.cmdLogger
	if cmdLogger == nil {
		var err error
		cmdLogger, err = store.NewWriter(c.lg.Named("writer"), c.storage, store.WriterCfg{
			Dir:              c.cfg.Output,
			EncryptionMethod: c.cfg.EncryptionMethod,
			EncryptionKey:    c.cfg.encryptionKey,
			Compress:         c.cfg.Compress,
		})
		if err != nil {
			c.lg.Error("failed to create capture writer", zap.Error(err))
			return
		}
	}
	// Flush all buffers even if the context is timeout.
	for buf := range bufCh {
		if _, err := cmdLogger.Write(buf.Bytes()); err != nil {
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
	filteredCmds := c.filteredCmds
	storage := c.storage
	c.Unlock()
	// Write meta outside of the lock to avoid affecting QPS.
	c.writeMeta(storage, time.Since(startTime), capturedCmds, filteredCmds)
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
		if lex.IsSensitiveSQL(hack.String(command.Payload[1:])) {
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

func (c *capture) writeMeta(storage storage.ExternalStorage, duration time.Duration, cmds, filteredCmds uint64) {
	meta := store.NewMeta(duration, cmds, filteredCmds, c.cfg.EncryptionMethod)
	if err := meta.Write(storage); err != nil {
		c.lg.Error("failed to write meta", zap.Error(err))
	}
}

func (c *capture) Progress() (float64, time.Time, bool, error) {
	c.Lock()
	defer c.Unlock()
	if c.status == statusIdle || c.cfg.Duration == 0 {
		return c.progress, c.endTime, true, c.err
	}
	progress := float64(time.Since(c.startTime)) / float64(c.cfg.Duration)
	if progress > 1 {
		progress = 1
	}
	return progress, c.endTime, false, c.err
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

func (c *capture) Wait() {
	c.wg.Wait()
}

func (c *capture) Stop(err error) {
	c.stop(err)
	c.wg.Wait()
}

func (c *capture) Close() {
	c.Stop(errors.New("shutting down"))
}
