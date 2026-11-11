// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestReplayConfigLogRedactsSecrets(t *testing.T) {
	cfg := ReplayConfig{
		Username:   "replay_user",
		Password:   "db-password",
		Input:      "/tmp/traffic,s3://bucket/in?access-key=AKIAINPUT&secret-access-key=SECRETINPUT&endpoint=http://user:ENDPOINTPASS@minio:9000,oss://bucket/in2?access-key=OSSIN&secret-access-key=OSSSECRET",
		OutputPath: "s3://container/out?endpoint=http://user:OUTPASS@host&secret-access-key=OUTSECRET",
		KeyFile:    "/var/lib/tiproxy/traffic.key",
		Addr:       "127.0.0.1:4000",
	}

	enc := zapcore.NewMapObjectEncoder()
	zap.Any("config", cfg).AddTo(enc)
	logged, err := json.Marshal(enc.Fields)
	require.NoError(t, err)
	text := string(logged)
	require.NotContains(t, text, "db-password")
	require.NotContains(t, text, "SECRETINPUT")
	require.NotContains(t, text, "OSSSECRET")
	require.NotContains(t, text, "OUTSECRET")
	require.NotContains(t, text, "ENDPOINTPASS")
	require.NotContains(t, text, "OUTPASS")
	require.NotContains(t, text, "access-key")
	require.Contains(t, text, "s3://bucket/in")
	require.Contains(t, text, "oss://bucket/in2")
	require.Contains(t, text, "s3://container/out")
	require.Contains(t, text, "replay_user")
	require.Contains(t, text, "127.0.0.1:4000")
	require.Contains(t, text, "/var/lib/tiproxy/traffic.key")
	require.Contains(t, text, "/tmp/traffic")

	tidbEnc := logger.NewTiDBEncoder(zap.NewProductionEncoderConfig())
	buf, err := tidbEnc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "start replay", Time: time.Now()}, []zapcore.Field{
		zap.Any("config", cfg),
	})
	require.NoError(t, err)
	defer buf.Free()
	line := buf.String()
	require.Contains(t, line, `[config="`)
	require.NotContains(t, line, "[config={")
	require.NotContains(t, line, "SECRETINPUT")
	require.NotContains(t, line, "ENDPOINTPASS")
	require.NotContains(t, line, "OUTPASS")
	require.NotContains(t, line, "db-password")

	cfg.DynamicInput = true
	cfg.Input = "http://user:SECRETINPUT@example.com/not-a-storage"
	storages, err := cfg.Validate()
	for _, storage := range storages {
		storage.Close()
	}
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SECRETINPUT")

	cfg.Input = "http://user:SECRETINPUT@example.com/a%zz"
	storages, err = cfg.Validate()
	for _, storage := range storages {
		storage.Close()
	}
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SECRETINPUT")
	require.Contains(t, err.Error(), "invalid URL escape")

	dir := t.TempDir()
	cfg.Input = dir + ",http://user:SECRETINPUT@example.com/a%zz"
	cfg.Format = "audit_log_plugin"
	cfg.ReplayerCount = 1
	storages, err = cfg.Validate()
	for _, storage := range storages {
		storage.Close()
	}
	require.Nil(t, storages)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SECRETINPUT")
	require.Contains(t, err.Error(), "invalid URL escape")

	query := url.Values{}
	query.Set("endpoint", "http://user:ENDPOINTPASS@minio:9000/%zz")
	query.Set("access-key", "AKIA")
	query.Set("secret-access-key", "TOPSECRET")
	cfg.DynamicInput = false
	cfg.Input = "s3://bucket/prefix?" + query.Encode()
	storages, err = cfg.Validate()
	for _, storage := range storages {
		storage.Close()
	}
	require.Error(t, err)
	require.NotContains(t, err.Error(), "ENDPOINTPASS")
	require.NotContains(t, err.Error(), "TOPSECRET")
	require.Contains(t, err.Error(), "invalid URL escape")

	plain := publicStorageErr(errors.New("storage http not support yet"))
	require.EqualError(t, plain, "storage http not support yet")

	require.Equal(t, "s3://bucket/in", RedactStorageURL("s3://bucket/in?access-key=AKIAINPUT&secret-access-key=SECRETINPUT&endpoint=http://user:ENDPOINTPASS@minio:9000"))
	require.Equal(t, "s3://bucket/in", RedactStorageURL("s3://user:SECRET@bucket/in"))
	require.Equal(t, "/tmp/traffic", RedactStorageURL("/tmp/traffic"))
	azure := RedactStorageURL("azure://container/out?account-name=acct&account-key=ACCOUNTKEY&sas-token=SASTOKEN")
	require.Equal(t, "azure://container/out", azure)
}
