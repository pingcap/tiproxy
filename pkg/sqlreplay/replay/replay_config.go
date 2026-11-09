// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/report"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"go.uber.org/zap/zapcore"
)

type ReplayConfig struct {
	Format   cmd.TrafficFormat
	Input    string
	Username string
	Password string `json:"-"`
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
	ReplayerCount uint64
	// ReplayerIndex is the index of this replayer among all replayers.
	ReplayerIndex uint64
	// OutputPath is the path to output replayed sql.
	OutputPath string
	// Addr is the downstream address to connect to
	Addr string
	// FilterCommandWithRetry indicates whether to filter out commands that are retries according to the audit log.
	FilterCommandWithRetry bool
	// UserAllowlist is only used for audit log plugin format. When non-empty, lines whose [USER=...] value
	// is not in this list are ignored (comma-separated in HTTP form; repeated or comma-separated CLI flag).
	// Matching is case-insensitive; HTTP form values are stored lowercased, and the audit decoder lowercases for lookup.
	UserAllowlist []string
	// WaitOnEOF indicates whether the replayer waits for the next file when no more files.
	WaitOnEOF bool
	// the following fields are for testing
	readers           []cmd.LineReader
	report            report.Report
	connCreator       conn.ConnCreator
	abortThreshold    int64
	slowDownThreshold int64
	slowDownFactor    time.Duration
	reportLogInterval time.Duration
}

func (cfg *ReplayConfig) Validate() ([]storeapi.Storage, error) {
	if cfg.Input == "" {
		return nil, errors.New("input is required")
	}
	inputs := strings.Split(cfg.Input, ",")
	if len(inputs) > 1 && !cfg.Format.IsAuditLogFormat() {
		return nil, errors.New("only `audit_log_plugin` format supports multiple input files")
	}
	var storages []storeapi.Storage
	var err error
	for _, input := range inputs {
		var inputStorage storeapi.Storage
		if cfg.DynamicInput {
			input, err = getDirForInput(input)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid input url %s", redactStorageList(input))
			}
		}
		inputStorage, err = store.NewStorage(input)
		if err != nil {
			for _, s := range storages {
				s.Close()
			}
			return nil, errors.Wrapf(err, "invalid input %s", redactStorageList(input))
		}
		storages = append(storages, inputStorage)
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
	case cmd.FormatAuditLogPlugin, cmd.FormatAuditLogExtension, cmd.FormatNative, "":
	default:
		return storages, errors.Errorf("invalid traffic file format %s", cfg.Format)
	}
	// Maybe there's a time bias between TiDB and TiProxy, so add one minute.
	now := time.Now()
	if cfg.StartTime.IsZero() {
		return storages, errors.New("start time is not specified")
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
	if !cfg.Format.IsAuditLogFormat() && !cfg.CommandEndTime.IsZero() {
		return storages, errors.New("command end time is only supported for `audit_log_plugin` format")
	}

	if cfg.Format == cmd.FormatAuditLogExtension {
		if cfg.PSCloseStrategy == cmd.PSCloseStrategyDirected {
			return storages, errors.New("prepared statement directed close strategy is not supported for audit log plugin v2 format")
		}
		if cfg.FilterCommandWithRetry {
			return storages, errors.New("filtering commands with retry is not supported for audit log plugin v2 format")
		}
	}

	if cfg.DynamicInput {
		if len(storages) != 1 {
			return storages, errors.New("dynamic input cannot be enabled with more than one input")
		}
		if cfg.ReplayerCount <= 0 {
			return storages, errors.New("dynamic input requires a valid replayer count")
		}
		if cfg.ReplayerIndex >= cfg.ReplayerCount {
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
		// Allow empty file.
		stat, err2 := file.Stat()
		if err2 != nil {
			return errors.Wrapf(err, "failed to stat checkpoint file %s", cfg.CheckPointFilePath)
		}
		if stat.Size() != 0 {
			return errors.Wrapf(err, "failed to decode checkpoint file %s", cfg.CheckPointFilePath)
		}
	}

	if state.CurCmdTs > 0 {
		cfg.CommandStartTime = time.Unix(0, state.CurCmdTs)
	}
	// Only load `CommandEndTime` for `audit_log_plugin` format, or it'll not pass validation.
	if state.CurCmdEndTs > 0 && cfg.Format.IsAuditLogFormat() {
		cfg.CommandEndTime = time.Unix(0, state.CurCmdEndTs)
	}
	return nil
}

// MarshalLogObject writes the replay config without the TiDB password or
// credentials embedded in storage URLs. zap.Any uses this instead of reflection.
func (cfg ReplayConfig) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	view := cfg
	view.Password = ""
	view.encryptionKey = nil
	view.Input = redactStorageList(cfg.Input)
	view.OutputPath = ast.RedactURL(cfg.OutputPath)
	return writeLogObject(enc, view)
}

func redactStorageList(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	for i, part := range parts {
		parts[i] = ast.RedactURL(strings.TrimSpace(part))
	}
	return strings.Join(parts, ",")
}

func writeLogObject(enc zapcore.ObjectEncoder, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err := enc.AddReflected(key, fields[key]); err != nil {
			return err
		}
	}
	return nil
}
