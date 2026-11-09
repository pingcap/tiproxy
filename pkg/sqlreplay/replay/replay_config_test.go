// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestReplayConfigLogRedactsSecrets(t *testing.T) {
	cfg := ReplayConfig{
		Username:   "replay_user",
		Password:   "db-password",
		Input:      "s3://bucket/in?access-key=AKIAINPUT&secret-access-key=SECRETINPUT,oss://bucket/in2?access-key=OSSIN&secret-access-key=OSSSECRET",
		OutputPath: "azure://container/out?account-name=acct&account-key=ACCOUNTKEY&sas-token=SASTOKEN",
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
	require.NotContains(t, text, "ACCOUNTKEY")
	require.NotContains(t, text, "SASTOKEN")
	require.Contains(t, text, "xxxxxx")
	require.Contains(t, text, "replay_user")
	require.Contains(t, text, "127.0.0.1:4000")
	require.Contains(t, text, "/var/lib/tiproxy/traffic.key")

	cfg.DynamicInput = true
	cfg.Input = "http://user:SECRETINPUT@example.com/not-a-storage"
	storages, err := cfg.Validate()
	for _, storage := range storages {
		storage.Close()
	}
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SECRETINPUT")
}
