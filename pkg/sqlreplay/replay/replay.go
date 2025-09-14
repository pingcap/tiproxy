// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"crypto/tls"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/report"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

const (
	// maxPendingExceptions is the maximum number of pending exceptions for all connections.
	maxPendingExceptions = 1024
	// slowDownThreshold is the threshold of pending commands to slow down. Following constants are tested with TPCC.
	slowDownThreshold = 1 << 18
	// slowDownFactor is the factor to slow down when there are too many pending commands.
	slowDownFactor = 100 * time.Nanosecond
	// abortThreshold is the threshold of pending commands to abort.
	abortThreshold = 1 << 21
	minSpeed       = 0.1
	maxSpeed       = 10.0
)

type Replay interface {
	// Start starts the replay
	Start(cfg ReplayConfig, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) error
	// Wait for the job done.
	Wait()
	// Stop stops the replay
	Stop(err error)
	// Progress returns the progress of the replay job
	Progress() (float64, time.Time, bool, error)
	// Close closes the replay
	Close()
}

type ReplayConfig struct {
	Format   string
	Input    string
	Username string
	Password string
	KeyFile  string
	// It's specified when executing with the statement `TRAFFIC REPLAY` so that all TiProxy instances
	// use the same start time and the time acts as the job ID.
	StartTime     time.Time
	Speed         float64
	ReadOnly      bool
	encryptionKey []byte
	// CommandStartTime is the start time of the command being replayed. It's different from StartTime,
	// which means the start time of the whole replay job.
	CommandStartTime time.Time
	// the following fields are for testing
	readers           []cmd.LineReader
	report            report.Report
	connCreator       conn.ConnCreator
	abortThreshold    int64
	slowDownThreshold int64
	slowDownFactor    time.Duration
}

func (cfg *ReplayConfig) Validate() ([]storage.ExternalStorage, error) {
	if cfg.Input == "" {
		return nil, errors.New("input is required")
	}
	inputs := strings.Split(cfg.Input, ",")
	if len(inputs) > 1 && cfg.Format != cmd.FormatAuditLogPlugin {
		return nil, errors.New("only `audit_log_plugin` format supports multiple input files")
	}
	var storages []storage.ExternalStorage
	var err error
	for _, input := range inputs {
		var storage storage.ExternalStorage
		storage, err = store.NewStorage(input)
		if err != nil {
			for _, s := range storages {
				s.Close()
			}
			return nil, errors.Wrapf(err, "invalid input %s", input)
		}
		storages = append(storages, storage)
	}
	if cfg.Username == "" {
		return storages, errors.New("username is required")
	}
	if cfg.Speed == 0 {
		cfg.Speed = 1
	} else if cfg.Speed < minSpeed || cfg.Speed > maxSpeed {
		return storages, errors.Errorf("speed should be between %f and %f", minSpeed, maxSpeed)
	}
	switch cfg.Format {
	case cmd.FormatAuditLogPlugin, cmd.FormatNative, "":
	default:
		return storages, errors.Errorf("invalid traffic file format %s", cfg.Format)
	}
	// Maybe there's a time bias between TiDB and TiProxy, so add one minute.
	now := time.Now()
	if cfg.StartTime.IsZero() {
		return storages, errors.New("start time is not specified")
	} else if now.Add(time.Minute).Before(cfg.StartTime) {
		return storages, errors.New("start time should not be in the future")
	} else if cfg.StartTime.Add(time.Minute).Before(now) {
		return storages, errors.New("start time should not be in the past")
	}
	if cfg.abortThreshold == 0 {
		cfg.abortThreshold = abortThreshold
	}
	if cfg.slowDownThreshold == 0 {
		cfg.slowDownThreshold = slowDownThreshold
	}
	if cfg.slowDownFactor == 0 {
		cfg.slowDownFactor = slowDownFactor
	}
	return storages, nil
}

type replay struct {
	sync.Mutex
	cfg              ReplayConfig
	meta             store.Meta
	storages         []storage.ExternalStorage
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
	decodedCmds      atomic.Uint64
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
	storages, err := cfg.Validate()
	if err != nil {
		for _, s := range storages {
			s.Close()
		}
		return err
	}

	r.Lock()
	defer r.Unlock()
	r.cfg = cfg
	r.storages = storages
	r.meta = *r.readMeta()
	r.startTime = cfg.StartTime
	r.endTime = time.Time{}
	r.progress = 0
	r.decodedCmds.Store(0)
	r.err = nil
	r.replayStats.Reset()
	r.exceptionCh = make(chan conn.Exception, maxPendingExceptions)
	r.closeCh = make(chan uint64, maxPendingExceptions)
	key, err := store.LoadEncryptionKey(r.meta.EncryptMethod, cfg.KeyFile)
	if err != nil {
		return errors.Wrapf(err, "failed to load encryption key")
	}
	r.cfg.encryptionKey = key

	hsHandler = NewHandshakeHandler(hsHandler)
	r.connCreator = cfg.connCreator
	if r.connCreator == nil {
		r.connCreator = func(connID uint64) conn.Conn {
			return conn.NewConn(r.lg.Named("conn"), r.cfg.Username, r.cfg.Password, backendTLSConfig, hsHandler, r.idMgr,
				connID, bcConfig, r.exceptionCh, r.closeCh, cfg.ReadOnly, &r.replayStats)
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
		StartTime: r.startTime,
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
	// cfg.readers is set in tests
	readers, err := r.constructReaders()
	if err != nil {
		r.stop(err)
		return
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()
	decoder, err := r.constructDecoders(readers)
	if err != nil {
		r.stop(err)
		return
	}

	var captureStartTs, replayStartTs time.Time
	conns := make(map[uint64]conn.Conn) // both alive and dead connections
	connCount := 0                      // alive connection count
	maxPendingCmds := int64(0)
	totalWaitTime := time.Duration(0)
	for ctx.Err() == nil {
		for hasCloseEvent := true; hasCloseEvent; {
			select {
			case id := <-r.closeCh:
				r.closeConn(id, conns, &connCount)
			default:
				hasCloseEvent = false
			}
		}

		var command *cmd.Command
		if command, err = decoder.Decode(); err != nil {
			if errors.Is(err, io.EOF) {
				r.lg.Info("replay reads EOF", zap.Stringers("reader", readers))
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
			if pendingCmds > maxPendingCmds {
				maxPendingCmds = pendingCmds
			}
			metrics.ReplayPendingCmdsGauge.Set(float64(pendingCmds))
			// If slowing down still doesn't help, abort the replay to avoid OOM.
			if pendingCmds > r.cfg.abortThreshold {
				err = errors.Errorf("too many pending commands, quit replay")
				r.lg.Error("too many pending commands, quit replay", zap.Int64("pending_cmds", pendingCmds))
				break
			}

			// Do not calculate the wait time by the duration since last command because the go scheduler
			// may wait a little bit longer than expected, and then the difference becomes larger and larger.
			expectedInterval := command.StartTs.Sub(captureStartTs)
			if r.cfg.Speed != 1 {
				expectedInterval = time.Duration(float64(expectedInterval) / r.cfg.Speed)
			}
			expectedInterval = max(time.Until(replayStartTs.Add(expectedInterval)), 0)
			// If there are too many pending commands, slow it down to reduce memory usage.
			if pendingCmds > r.cfg.slowDownThreshold {
				extraWait := time.Duration(pendingCmds-r.cfg.slowDownThreshold) * r.cfg.slowDownFactor
				totalWaitTime += extraWait
				expectedInterval += extraWait
				metrics.ReplayWaitTime.Set(float64(totalWaitTime.Nanoseconds()))
			}
			if expectedInterval > time.Microsecond {
				select {
				case <-ctx.Done():
				case <-time.After(expectedInterval):
				}
			}
		}
		if ctx.Err() == nil {
			r.executeCmd(ctx, command, conns, &connCount)
		}
	}
	r.lg.Info("finished decoding commands, draining connections", zap.Int64("max_pending_cmds", maxPendingCmds),
		zap.Duration("total_wait_time", totalWaitTime), zap.Int("alive_conns", connCount))

	// Notify the connections that the commands are finished.
	for _, conn := range conns {
		if conn != nil && !reflect.ValueOf(conn).IsNil() {
			conn.Stop()
		}
	}

	// Wait until all connections are closed before logging the finished message.
	// Besides, drain all close events to avoid blocking connections at writing to channels.
	for connCount > 0 {
		id := <-r.closeCh
		r.closeConn(id, conns, &connCount)
	}
	r.stop(err)
}

func (r *replay) constructDecoders(readers []cmd.LineReader) (decoder, error) {
	var decoders []decoder
	for _, reader := range readers {
		cmdDecoder := cmd.NewCmdDecoder(r.cfg.Format)
		// It's better to filter out the commands in `readCommands` instead of `Decoder`. However,
		// the connection state is maintained in decoder. Filtering out commands here will make it'
		// impossible for decoder to know whether `use xxx` will be executed, and thus cannot maintain
		// the current session state correctly.
		cmdDecoder.SetCommandStartTime(r.cfg.CommandStartTime)
		decoders = append(decoders, newSingleDecoder(cmdDecoder, reader))
	}
	if len(decoders) == 0 {
		return nil, errors.New("no decoder")
	}
	if len(decoders) == 1 {
		return decoders[0], nil
	}

	return newMergeDecoder(decoders...), nil
}

func (r *replay) constructReaders() ([]cmd.LineReader, error) {
	readers := r.cfg.readers
	inputs := strings.Split(r.cfg.Input, ",")
	if readers == nil {
		if len(inputs) != len(r.storages) {
			return nil, errors.Errorf("input count %d doesn't match storage count %d", len(inputs), len(r.storages))
		}

		for i, storage := range r.storages {
			reader, err := store.NewReader(r.lg.Named("loader"), storage, store.ReaderCfg{
				Format:           r.cfg.Format,
				Dir:              inputs[i],
				EncryptionKey:    r.cfg.encryptionKey,
				EncryptionMethod: r.meta.EncryptMethod,
				CommandStartTime: r.cfg.CommandStartTime,
			})
			if err != nil {
				for _, r := range readers {
					r.Close()
				}
				return nil, err
			}
			readers = append(readers, reader)
		}
	}
	return readers, nil
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
	r.decodedCmds.Add(1)
}

func (r *replay) closeConn(connID uint64, conns map[uint64]conn.Conn, connCount *int) {
	// Keep the disconnected connections in the map to reject subsequent commands with the same connID,
	// but release memory as much as possible.
	if conn, ok := conns[connID]; ok && conn != nil && !reflect.ValueOf(conn).IsNil() {
		conns[connID] = nil
		*connCount--
	}
}

func (r *replay) Progress() (float64, time.Time, bool, error) {
	pendingCmds := r.replayStats.PendingCmds.Load()
	r.Lock()
	defer r.Unlock()
	if r.meta.Cmds > 0 {
		r.progress = float64(r.decodedCmds.Load()-uint64(pendingCmds)) / float64(r.meta.Cmds)
	}
	return r.progress, r.endTime, r.startTime.IsZero(), r.err
}

func (r *replay) readMeta() *store.Meta {
	m := new(store.Meta)
	if r.cfg.Format == cmd.FormatNative || r.cfg.Format == "" {
		// The `native` format always has only one storage.
		if err := m.Read(r.storages[0]); err != nil {
			r.lg.Error("read meta failed", zap.Error(err))
		}
	}
	return m
}

func (r *replay) stop(err error) {
	r.Lock()
	defer r.Unlock()

	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.endTime = time.Now()
	// decodedCmds - pendingCmds may be greater than replayedCmds because if a connection is closed unexpectedly,
	// the pending commands of that connection are discarded. We calculate the progress based on decodedCmds - pendingCmds.
	replayedCmds := r.replayStats.ReplayedCmds.Load()
	pendingCmds := r.replayStats.PendingCmds.Load()
	filteredCmds := r.replayStats.FilteredCmds.Load()
	decodedCmds := r.decodedCmds.Load()
	if pendingCmds != 0 {
		r.lg.Warn("pending command count is not 0", zap.Int64("pending_cmds", pendingCmds))
	}
	fields := []zap.Field{
		zap.Time("start_time", r.startTime),
		zap.Time("end_time", r.endTime),
		zap.Uint64("decoded_cmds", decodedCmds),
		zap.Uint64("replayed_cmds", replayedCmds),
		zap.Uint64("filtered_cmds", filteredCmds),
	}
	if r.meta.Cmds > 0 {
		r.progress = float64(decodedCmds) / float64(r.meta.Cmds)
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
	metrics.ReplayPendingCmdsGauge.Set(0)
	metrics.ReplayWaitTime.Set(0)
	if r.storages != nil && !reflect.ValueOf(r.storages).IsNil() {
		for _, s := range r.storages {
			s.Close()
		}
		r.storages = nil
	}
}

func (r *replay) Wait() {
	r.wg.Wait()
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
	if r.report != nil {
		r.report.Close()
	}
}
