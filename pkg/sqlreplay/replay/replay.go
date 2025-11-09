// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/report"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

const (
	// maxPendingExceptions is the maximum number of pending exceptions for all connections.
	maxPendingExceptions = 1024
	// maxPendingCloseRequests is the maximum number of pending connection close requests.
	// Make it big enough in case all the connections are closed all at once.
	maxPendingCloseRequests = 1 << 16
	// maxPendingExecInfo is the maximum number of pending exec info for all connections.
	maxPendingExecInfo = 1 << 16
	// slowDownThreshold is the threshold of pending commands to slow down. Following constants are tested with TPCC.
	slowDownThreshold = 1 << 18
	// slowDownFactor is the factor to slow down when there are too many pending commands.
	slowDownFactor = 100 * time.Nanosecond
	// abortThreshold is the threshold of pending commands to abort.
	abortThreshold    = 1 << 21
	minSpeed          = 0.1
	maxSpeed          = 10.0
	reportLogInterval = 10 * time.Second

	checkpointSaveInterval = 100 * time.Millisecond
	stateSaveRetryInterval = 10 * time.Second

	// The output log size in MB.
	outputLogSize = 10
	// The buffer size of the output log.
	outputBufferSize = 4 * 1024
	// The flush interval of the output log.
	outputLogFlushIntvl = 10 * time.Second
)

var (
	// chanBufForEachDecoder is the buffer size for each reader's channel. If it's set to -1,
	// it'll use synchronized implementation for each reader.
	chanBufForEachDecoder = 64
)

type Replay interface {
	// Start starts the replay
	Start(cfg ReplayConfig, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) error
	// Wait for the job done.
	Wait()
	// Stop stops the replay
	Stop(err error, graceful bool)
	// Progress returns the progress of the replay job
	Progress() (float64, time.Time, time.Time, time.Time, bool, error)
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
	// CommandEndTime is the end time of the command being replayed. This config is only valid for audit
	// log plugin format.
	CommandEndTime time.Time
	// IgnoreErrs indicates whether to ignore decoding errors.
	// The errors are just printed if true, otherwise the replayer stops.
	IgnoreErrs bool
	// BufSize is the size of the buffer for reordering commands from audit files. 0 means no buffering.
	BufSize int
	// PSCloseStrategy defines when to close the prepared statements.
	PSCloseStrategy cmd.PSCloseStrategy
	// DryRun indicates whether to actually execute the commands.
	DryRun bool
	// CheckPointFilePath is the path to the file that stores the current state of the replay
	CheckPointFilePath string
	// Dynamic defines whether the input is dynamic, e.g. a path prefix.
	DynamicInput bool
	// ReplayerCount is the total number of replayers which share the same dynamic input. The count
	// and index is used to determine whether this replayer should process a new
	ReplayerCount int
	// ReplayerIndex is the index of this replayer among all replayers.
	ReplayerIndex int
	// OutputPath is the path to output replayed sql.
	OutputPath string
	// the following fields are for testing
	readers           []cmd.LineReader
	report            report.Report
	connCreator       conn.ConnCreator
	abortThreshold    int64
	slowDownThreshold int64
	slowDownFactor    time.Duration
	reportLogInterval time.Duration
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
		if cfg.DynamicInput {
			input, err = getDirForInput(input)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid input url %s", input)
			}
		}
		storage, err = store.NewStorage(input)
		if err != nil {
			for _, s := range storages {
				s.Close()
			}
			return nil, errors.Wrapf(err, "invalid input %s", input)
		}
		storages = append(storages, storage)
	}
	if !cfg.DryRun && cfg.Username == "" {
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
	if cfg.reportLogInterval == 0 {
		cfg.reportLogInterval = reportLogInterval
	}
	if cfg.Format == cmd.FormatNative && cfg.PSCloseStrategy != cmd.PSCloseStrategyDirected {
		return storages, errors.New("only `directed` prepared statement close strategy is supported for `native` format")
	}
	switch cfg.PSCloseStrategy {
	case cmd.PSCloseStrategyAlways, cmd.PSCloseStrategyDirected, cmd.PSCloseStrategyNever:
	default:
		return storages, errors.Errorf("invalid prepared statement close strategy %s", cfg.PSCloseStrategy)
	}
	if cfg.Format != cmd.FormatAuditLogPlugin && !cfg.CommandEndTime.IsZero() {
		return storages, errors.New("command end time is only supported for `audit_log_plugin` format")
	}

	if cfg.DynamicInput {
		if len(storages) != 1 {
			return storages, errors.New("dynamic input cannot be enabled with more than one input")
		}
		if cfg.ReplayerCount <= 0 {
			return storages, errors.New("dynamic input requires a valid replayer count")
		}
		if cfg.ReplayerIndex < 0 || cfg.ReplayerIndex >= cfg.ReplayerCount {
			return storages, errors.New("dynamic input requires a valid replayer index")
		}
	}
	return storages, nil
}

// LoadFromCheckpoint loads the config from the checkpoint file.
func (cfg *ReplayConfig) LoadFromCheckpoint() error {
	if len(cfg.CheckPointFilePath) == 0 {
		return nil
	}

	file, err := os.Open(cfg.CheckPointFilePath)
	if err != nil {
		// Allow the file to not exist.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var state replayCheckpoint
	if err := decoder.Decode(&state); err != nil {
		return errors.Wrapf(err, "failed to decode checkpoint file %s", cfg.CheckPointFilePath)
	}

	if state.CurCmdTs > 0 {
		cfg.CommandStartTime = time.Unix(0, state.CurCmdTs)
	}
	// Only load `CommandEndTime` for `audit_log_plugin` format, or it'll not pass validation.
	if state.CurCmdEndTs > 0 && cfg.Format == cmd.FormatAuditLogPlugin {
		cfg.CommandEndTime = time.Unix(0, state.CurCmdEndTs)
	}
	return nil
}

type replay struct {
	sync.Mutex
	cfg              ReplayConfig
	meta             store.Meta
	storages         []storage.ExternalStorage
	replayStats      conn.ReplayStats
	idMgr            *id.IDManager
	exceptionCh      chan conn.Exception
	closeConnCh      chan uint64
	execInfoCh       chan conn.ExecInfo
	gracefulStop     atomic.Bool
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
	err := cfg.LoadFromCheckpoint()
	if err != nil {
		return err
	}
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
	r.closeConnCh = make(chan uint64, maxPendingCloseRequests)
	r.execInfoCh = make(chan conn.ExecInfo, maxPendingExecInfo)
	key, err := store.LoadEncryptionKey(r.meta.EncryptMethod, cfg.KeyFile)
	if err != nil {
		return errors.Wrapf(err, "failed to load encryption key")
	}
	r.cfg.encryptionKey = key

	hsHandler = NewHandshakeHandler(hsHandler)
	r.connCreator = cfg.connCreator
	if r.connCreator == nil {
		if cfg.DryRun {
			r.connCreator = func(connID uint64, _ uint64) conn.Conn {
				return &nopConn{
					connID:     connID,
					closeCh:    r.closeConnCh,
					execInfoCh: r.execInfoCh,
					stats:      &r.replayStats,
				}
			}
		} else {
			r.connCreator = func(connID uint64, upstreamConnID uint64) conn.Conn {
				return conn.NewConn(r.lg.Named("conn"), conn.ConnOpts{
					Username:         r.cfg.Username,
					Password:         r.cfg.Password,
					BackendTLSConfig: backendTLSConfig,
					HsHandler:        hsHandler,
					IdMgr:            r.idMgr,
					ConnID:           connID,
					UpstreamConnID:   upstreamConnID,
					BcConfig:         bcConfig,
					ExceptionCh:      r.exceptionCh,
					CloseCh:          r.closeConnCh,
					ExecInfoCh:       r.execInfoCh,
					ReplayStats:      &r.replayStats,
					Readonly:         cfg.ReadOnly,
				})
			}
		}
	}
	r.report = cfg.report
	if r.report == nil {
		if cfg.DryRun {
			r.report = &mockReport{exceptionCh: r.exceptionCh}
		} else {
			backendConnCreator := func() conn.BackendConn {
				return conn.NewBackendConn(r.lg.Named("be"), r.idMgr.NewID(), hsHandler, bcConfig, backendTLSConfig, r.cfg.Username, r.cfg.Password)
			}
			r.report = report.NewReport(r.lg.Named("report"), r.exceptionCh, backendConnCreator)
		}
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
	r.wg.RunWithRecover(func() {
		r.reportLoop(childCtx)
	}, nil, r.lg)
	if len(r.cfg.CheckPointFilePath) > 0 {
		r.wg.RunWithRecover(func() {
			r.saveCheckpointLoop(childCtx)
		}, nil, r.lg)
	}
	r.wg.RunWithRecover(func() {
		r.recordExecInfoLoop()
	}, nil, r.lg)
	return nil
}

func (r *replay) readCommands(ctx context.Context) {
	var decoder decoder
	var err error

	if !r.cfg.DynamicInput {
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
		decoder, err = r.constructStaticDecoder(ctx, readers)
		if err != nil {
			r.stop(err)
			return
		}
	} else {
		decoder, err = r.constructDynamicDecoder(ctx)
		if err != nil {
			r.stop(err)
			return
		}
	}

	var captureStartTs, replayStartTs time.Time
	conns := make(map[uint64]conn.Conn) // both alive and dead connections
	connCount := 0                      // alive connection count
	maxPendingCmds := int64(0)
	extraWaitTime := time.Duration(0)
	for ctx.Err() == nil && !r.gracefulStop.Load() {
		for hasCloseEvent := true; hasCloseEvent; {
			select {
			case id := <-r.closeConnCh:
				r.closeConn(id, conns, &connCount)
			default:
				hasCloseEvent = false
			}
		}

		var command *cmd.Command
		if command, err = decoder.Decode(); err != nil {
			if errors.Is(err, io.EOF) {
				r.lg.Info("replay reads EOF")
				err = nil
				break
			}
			if r.cfg.IgnoreErrs {
				r.lg.Error("failed to decode command", zap.Error(err))
				err = nil
				continue
			} else {
				break
			}
		}
		r.replayStats.CurCmdTs.Store(command.StartTs.UnixNano())
		if !command.EndTs.IsZero() {
			r.replayStats.CurCmdEndTs.Store(command.EndTs.UnixNano())
		} else {
			// fallback to StartTs if EndTs is not available.
			r.replayStats.CurCmdEndTs.Store(command.StartTs.UnixNano())
		}
		if captureStartTs.IsZero() {
			// first command
			captureStartTs = command.StartTs
			replayStartTs = time.Now()
			r.replayStats.ReplayStartTs.Store(replayStartTs.UnixNano())
			r.replayStats.FirstCmdTs.Store(command.StartTs.UnixNano())
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
				extraWaitTime += extraWait
				expectedInterval += extraWait
				metrics.ReplayWaitTime.Set(float64(extraWaitTime.Nanoseconds()))
				r.replayStats.ExtraWaitTime.Store(extraWaitTime.Nanoseconds())
			}
			if expectedInterval > time.Microsecond {
				r.replayStats.TotalWaitTime.Add(expectedInterval.Nanoseconds())
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
		zap.Duration("extra_wait_time", extraWaitTime),
		zap.Int("alive_conns", connCount),
		zap.Time("last_cmd_start_ts", time.Unix(0, r.replayStats.CurCmdTs.Load())),
		zap.Time("last_cmd_end_ts", time.Unix(0, r.replayStats.CurCmdEndTs.Load())))

	// Notify the connections that the commands are finished.
	for _, conn := range conns {
		if conn != nil && !reflect.ValueOf(conn).IsNil() {
			conn.Stop()
		}
		// Avoid the channel to be full.
		for closed := true; closed; {
			closed = false
			select {
			case id := <-r.closeConnCh:
				r.closeConn(id, conns, &connCount)
				closed = true
			default:
			}
		}
	}

	// Wait until all connections are closed before logging the finished message.
	// Besides, drain all close events to avoid blocking connections at writing to channels.
	for connCount > 0 {
		id := <-r.closeConnCh
		r.closeConn(id, conns, &connCount)
	}
	r.stop(err)
}

func (r *replay) constructMergeDecoders(ctx context.Context, readers []cmd.LineReader) (decoder, error) {
	var decoders []decoder

	for i, reader := range readers {
		decoder, err := r.constructDecoderForReader(ctx, reader, i)
		if err != nil {
			return nil, err
		}
		decoders = append(decoders, decoder)
	}
	if len(decoders) == 0 {
		return nil, errors.New("no decoder")
	}
	if len(decoders) == 1 {
		return decoders[0], nil
	}

	return newMergeDecoder(decoders...), nil
}

func (r *replay) constructDecoderForReader(ctx context.Context, reader cmd.LineReader, id int) (decoder, error) {
	idAllocator, err := cmd.NewConnIDAllocator(id)
	if err != nil {
		return nil, err
	}

	cmdDecoder := cmd.NewCmdDecoder(r.cfg.Format)
	// It's better to filter out the commands in `readCommands` instead of `Decoder`. However,
	// the connection state is maintained in decoder. Filtering out commands here will make it'
	// impossible for decoder to know whether `use xxx` will be executed, and thus cannot maintain
	// the current session state correctly.
	cmdDecoder.SetCommandStartTime(r.cfg.CommandStartTime)
	if auditLogDecoder, ok := cmdDecoder.(*cmd.AuditLogPluginDecoder); ok {
		auditLogDecoder.SetPSCloseStrategy(r.cfg.PSCloseStrategy)
		auditLogDecoder.SetIDAllocator(idAllocator)
		auditLogDecoder.SetCommandEndTime(r.cfg.CommandEndTime)
	}

	var decoder decoder
	decoder = newSingleDecoder(cmdDecoder, reader)
	if chanBufForEachDecoder > 0 {
		decoder = newBufferedDecoder(ctx, decoder, chanBufForEachDecoder, r.cfg.IgnoreErrs)
	}

	return decoder, nil
}

func (r *replay) constructStaticDecoder(ctx context.Context, readers []cmd.LineReader) (decoder, error) {
	decoder, err := r.constructMergeDecoders(ctx, readers)
	if err != nil {
		return nil, err
	}

	if r.cfg.BufSize > 0 {
		decoder = newBufferedDecoder(ctx, decoder, r.cfg.BufSize, r.cfg.IgnoreErrs)
	}
	return decoder, nil
}

func (r *replay) constructDynamicDecoder(ctx context.Context) (decoder, error) {
	decoder := newMergeDecoder()

	parsedURL, err := url.Parse(r.cfg.Input)
	if err != nil {
		return nil, err
	}
	watcher := store.NewDirWatcher(r.lg.Named("dir_watcher"), strings.TrimLeft(parsedURL.Path, "/"), func(filename string) error {
		h := fnv.New64a()
		// error is never returned for Hash
		_, _ = h.Write([]byte(filename))
		sum := h.Sum64()
		expectedReplayerIndex := int(sum) % r.cfg.ReplayerCount
		if expectedReplayerIndex != r.cfg.ReplayerIndex {
			r.lg.Info("dir watcher skip new directory for other replayer", zap.String("dir", filename),
				zap.Int("expected_replayer_index", expectedReplayerIndex),
				zap.Int("replayer_index", r.cfg.ReplayerIndex))
			return nil
		}

		// determine the start time filter for the new reader
		filterTime := time.Time{}
		if !r.cfg.CommandEndTime.IsZero() {
			filterTime = r.cfg.CommandEndTime
		}
		curCmdEndTs := r.replayStats.CurCmdEndTs.Load()
		if curCmdEndTs > 0 {
			t := time.Unix(0, curCmdEndTs)
			if t.After(filterTime) {
				filterTime = t
			}
		}

		r.lg.Info("dir watcher found new directory", zap.String("dir", filename), zap.Time("filter_time", filterTime))
		// We'll need to setup a new storage, because the `r.storages[0]` has a wrong path
		url, err := url.Parse(r.cfg.Input)
		if err != nil {
			return err
		}
		url.Path = filename
		s, err := store.NewStorage(url.String())
		if err != nil {
			return err
		}

		// Append the new storage to the list for closing later.
		r.Lock()
		defer r.Unlock()

		r.storages = append(r.storages, s)
		reader, err := r.constructReaderForDir(s, filename, filterTime)
		if err != nil {
			return err
		}
		newDecoder, err := r.constructDecoderForReader(ctx, reader, len(r.storages)-1)
		if err != nil {
			return err
		}

		decoder.AddDecoder(newDecoder)
		return nil
	}, r.storages[0])

	// Initial walk. We have to make sure the decoder is not empty before returning this function.
	err = watcher.WalkFiles(ctx)
	// If the first walk fails, return error directly.
	if err != nil {
		return nil, err
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		err := watcher.Watch(ctx)
		if err != nil {
			r.lg.Info("dir watcher exited", zap.Error(err))
		}
	}()

	if r.cfg.BufSize > 0 {
		return newBufferedDecoder(ctx, decoder, r.cfg.BufSize, r.cfg.IgnoreErrs), nil
	}
	return decoder, nil
}

func (r *replay) constructReaderForDir(storage storage.ExternalStorage, dir string, filterTime time.Time) (cmd.LineReader, error) {
	cfg := store.ReaderCfg{
		Format:             r.cfg.Format,
		Dir:                dir,
		EncryptionKey:      r.cfg.encryptionKey,
		EncryptionMethod:   r.meta.EncryptMethod,
		FileNameFilterTime: filterTime,
	}
	if r.cfg.CommandEndTime.IsZero() {
		cfg.FileNameFilterTime = r.cfg.CommandStartTime
	}
	reader, err := store.NewReader(r.lg.Named("loader"), storage, cfg)
	if err != nil {
		reader.Close()
		return nil, err
	}

	return reader, nil
}

func (r *replay) constructReaders() ([]cmd.LineReader, error) {
	readers := r.cfg.readers
	inputs := strings.Split(r.cfg.Input, ",")
	if readers == nil {
		if len(inputs) != len(r.storages) {
			return nil, errors.Errorf("input count %d doesn't match storage count %d", len(inputs), len(r.storages))
		}

		for i, storage := range r.storages {
			reader, err := r.constructReaderForDir(storage, inputs[i], r.cfg.CommandEndTime)
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
		conn = r.connCreator(command.ConnID, command.UpstreamConnID)
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
		delete(conns, connID)
		*connCount--
	}
}

func (r *replay) Progress() (float64, time.Time, time.Time, time.Time, bool, error) {
	pendingCmds := r.replayStats.PendingCmds.Load()
	r.Lock()
	defer r.Unlock()
	if r.meta.Cmds > 0 {
		r.progress = float64(r.decodedCmds.Load()-uint64(pendingCmds)) / float64(r.meta.Cmds)
	}
	curCmdTs := time.Unix(0, r.replayStats.CurCmdTs.Load())
	curCmdEndTs := time.Unix(0, r.replayStats.CurCmdEndTs.Load())
	return r.progress, r.endTime, curCmdTs, curCmdEndTs, r.startTime.IsZero(), r.err
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

func (r *replay) reportLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.reportLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			decodeElapsed := r.replayStats.CurCmdTs.Load() - r.replayStats.FirstCmdTs.Load()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			r.lg.Info("replay progress", zap.Uint64("replayed_cmds", r.replayStats.ReplayedCmds.Load()),
				zap.Int64("pending_cmds", r.replayStats.PendingCmds.Load()), // if too many, replay is slower than decode
				zap.Uint64("filtered_cmds", r.replayStats.FilteredCmds.Load()),
				zap.Uint64("decoded_cmds", r.decodedCmds.Load()),
				zap.Uint64("exceptions", r.replayStats.ExceptionCmds.Load()),
				zap.Duration("total_wait_time", time.Duration(r.replayStats.TotalWaitTime.Load())), // if too short, decode is low
				zap.Duration("extra_wait_time", time.Duration(r.replayStats.ExtraWaitTime.Load())), // if non-zero, replay is slow
				zap.Duration("replay_elapsed", time.Since(r.startTime)),
				zap.Duration("decode_elapsed", time.Duration(decodeElapsed)), // if shorter than replay_elapsed, decode is slow
				zap.Time("last_cmd_start_ts", time.Unix(0, r.replayStats.CurCmdTs.Load())),
				zap.Time("last_cmd_end_ts", time.Unix(0, r.replayStats.CurCmdEndTs.Load())),
				zap.String("sys_memory", fmt.Sprintf("%.2fMB", float64(m.Sys)/1024/1024)))
		}
	}
}

// replayCheckpoint is a struct to serialize and save the current state of replay
// on disk, so that the following execution can resume from the saved state.
type replayCheckpoint struct {
	CurCmdTs    int64 `json:"cur_cmd_ts"`
	CurCmdEndTs int64 `json:"cur_cmd_end_ts"`
}

func (r *replay) saveCheckpointLoop(ctx context.Context) {
	ticker := time.NewTicker(checkpointSaveInterval)
	defer ticker.Stop()

	file, err := os.OpenFile(r.cfg.CheckPointFilePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		r.lg.Error("open checkpoint file failed", zap.Error(err))
		return
	}
	defer file.Close()

	for {
		// Add an interval here to avoid printing too many logs when error occurs.
		if err != nil {
			time.Sleep(stateSaveRetryInterval)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err = r.saveCheckpointToFile(file)
			if err != nil {
				r.lg.Error("save current checkpoint failed", zap.Error(err))
				time.Sleep(stateSaveRetryInterval)
				continue
			}
		}
	}
}

func (r *replay) saveCheckpointToFile(file *os.File) error {
	state := r.fetchCurrentCheckpoint()
	err := file.Truncate(0)
	if err != nil {
		return errors.Wrapf(err, "truncate checkpoint file")
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrapf(err, "seek checkpoint file")
	}

	err = json.NewEncoder(file).Encode(state)
	if err != nil {
		return errors.Wrapf(err, "save current checkpoint")
	}
	return nil
}

func (r *replay) saveCurrentStateToFilePath(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return errors.Wrapf(err, "open state file %s", filePath)
	}
	defer file.Close()

	return r.saveCheckpointToFile(file)
}

func (r *replay) fetchCurrentCheckpoint() replayCheckpoint {
	return replayCheckpoint{
		CurCmdTs:    r.replayStats.CurCmdTs.Load(),
		CurCmdEndTs: r.replayStats.CurCmdEndTs.Load(),
	}
}

func (r *replay) recordExecInfoLoop() {
	var storage storage.ExternalStorage
	var writer io.WriteCloser
	if len(r.cfg.OutputPath) > 0 {
		var err error
		if storage, err = store.NewStorage(r.cfg.OutputPath); err != nil {
			r.lg.Error("failed to create storage for recording execution info", zap.Error(err))
		}
		if writer, err = store.NewWriter(r.lg.Named("writer"), storage, store.WriterCfg{Dir: r.cfg.OutputPath}); err != nil {
			r.lg.Error("failed to create writer for recording execution info", zap.Error(err))
		}
	}
	defer func() {
		if writer != nil && !reflect.ValueOf(writer).IsNil() {
			writer.Close()
		}
		if storage != nil && !reflect.ValueOf(storage).IsNil() {
			storage.Close()
		}
	}()

	// Iterate until the channel is closed, even if the context has been canceled.
	for info := range r.execInfoCh {
		if writer == nil || reflect.ValueOf(writer).IsNil() {
			continue
		}
		var sql string
		switch info.Command.Type {
		case pnet.ComStmtExecute:
			sql = info.Command.PreparedStmt
		case pnet.ComQuery:
			sql = hack.String(info.Command.Payload[1:])
			sql = parser.Normalize(sql, "ON")
		}
		if len(sql) > 0 {
			sql = strconv.Quote(sql)
			t := time.Now().Format("20060102 15:04:05")
			jsonStr := fmt.Sprintf("{\"sql\": \"%s\", \"db\": \"%s\", \"cost\": \"%d\", \"ex_time\": \"%s\"}\n", sql, info.Command.CurDB, info.CostTime.Milliseconds()/1000000.0, t)
			if _, err := writer.Write(hack.Slice(jsonStr)); err != nil {
				r.lg.Warn("failed to record execution info", zap.Error(err))
			}
		}
	}
}

func (r *replay) stop(err error) {
	r.Lock()
	defer r.Unlock()

	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	close(r.execInfoCh)
	r.endTime = time.Now()
	// decodedCmds - pendingCmds may be greater than replayedCmds because if a connection is closed unexpectedly,
	// the pending commands of that connection are discarded. We calculate the progress based on decodedCmds - pendingCmds.
	pendingCmds := r.replayStats.PendingCmds.Load()
	decodedCmds := r.decodedCmds.Load()
	if pendingCmds != 0 {
		r.lg.Warn("pending command count is not 0", zap.Int64("pending_cmds", pendingCmds))
	}
	decodeElapsed := r.replayStats.CurCmdTs.Load() - r.replayStats.FirstCmdTs.Load()
	fields := []zap.Field{
		zap.Time("start_time", r.startTime),
		zap.Time("end_time", r.endTime),
		zap.Time("command_start_time", r.cfg.CommandStartTime),
		zap.Time("command_end_time", r.cfg.CommandEndTime),
		zap.String("format", r.cfg.Format),
		zap.String("username", r.cfg.Username),
		zap.Bool("ignore_errs", r.cfg.IgnoreErrs),
		zap.Float64("speed", r.cfg.Speed),
		zap.Bool("read_only", r.cfg.ReadOnly),
		zap.Uint64("decoded_cmds", decodedCmds),
		zap.Uint64("replayed_cmds", r.replayStats.ReplayedCmds.Load()),
		zap.Uint64("filtered_cmds", r.replayStats.FilteredCmds.Load()),
		zap.Uint64("exceptions", r.replayStats.ExceptionCmds.Load()),
		zap.Duration("replay_elapsed", time.Since(r.startTime)),
		zap.Duration("decode_elapsed", time.Duration(decodeElapsed)),
		zap.Duration("extra_wait_time", time.Duration(r.replayStats.ExtraWaitTime.Load())),
		zap.Time("last_cmd_start_ts", time.Unix(0, r.replayStats.CurCmdTs.Load())),
		zap.Time("last_cmd_end_ts", time.Unix(0, r.replayStats.CurCmdEndTs.Load())),
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

func (r *replay) Stop(err error, graceful bool) {
	r.Lock()
	// already stopped
	if r.startTime.IsZero() {
		r.Unlock()
		return
	}
	r.err = err
	if graceful {
		r.gracefulStop.Store(true)
	} else if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.Unlock()
	r.wg.Wait()
}

func (r *replay) Close() {
	r.Stop(errors.New("shutting down"), false)
	if r.report != nil {
		r.report.Close()
	}
	// at this time, the save checkpoint loop and replay loop have exited. It's safe to update the latest
	// checkpoint file.
	if len(r.cfg.CheckPointFilePath) > 0 {
		err := r.saveCurrentStateToFilePath(r.cfg.CheckPointFilePath)
		if err != nil {
			r.lg.Error("save current state failed on close", zap.Error(err))
		}
	}
}

func getDirForInput(input string) (string, error) {
	parsedURL, err := url.Parse(input)
	if err != nil {
		return "", err
	}
	parsedURL.Path = filepath.Dir(parsedURL.Path)
	return parsedURL.String(), nil
}
