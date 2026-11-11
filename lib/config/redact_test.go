// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRedactedKeepsSecretsOutOfAPIAndLogs(t *testing.T) {
	cfg := NewConfig()
	cfg.Metering = mconfig.MeteringConfig{
		Type:     storage.ProviderTypeAzure,
		Bucket:   "metering-container",
		Prefix:   "tiproxy/metering",
		Endpoint: "https://user:sas@account.blob.core.windows.net/container?sas-token=sas-secret",
		Azure: &mconfig.MeteringAzureConfig{
			AccountName: "account",
			AccountKey:  "account-key-secret",
			SASToken:    "sas-secret",
		},
		AWS: &mconfig.MeteringAWSConfig{
			AssumeRoleARN:   "arn:aws:iam::123:role/metering",
			AccessKey:       "aws-access",
			SecretAccessKey: "aws-secret",
			SessionToken:    "aws-token",
		},
		OSS: &mconfig.MeteringOSSConfig{
			AccessKey:       "oss-access",
			SecretAccessKey: "oss-secret",
			SessionToken:    "oss-token",
		},
		COS: &mconfig.MeteringCOSConfig{
			AssumeRoleARN:   "qcs::cam::uin/1:roleName/metering",
			AccessKey:       "cos-access",
			SecretAccessKey: "cos-secret",
			SessionToken:    "cos-token",
		},
	}
	cfg.Security.EncryptionKeyPath = "/var/lib/tiproxy/traffic.key"

	redacted := cfg.Redacted()
	require.Equal(t, "sas-secret", cfg.Metering.Azure.SASToken)
	require.Equal(t, "aws-secret", cfg.Metering.AWS.SecretAccessKey)
	require.Equal(t, "account", redacted.Metering.Azure.AccountName)
	require.Empty(t, redacted.Metering.Azure.AccountKey)
	require.Empty(t, redacted.Metering.Azure.SASToken)
	require.Equal(t, "arn:aws:iam::123:role/metering", redacted.Metering.AWS.AssumeRoleARN)
	require.Empty(t, redacted.Metering.AWS.AccessKey)
	require.Empty(t, redacted.Metering.AWS.SecretAccessKey)
	require.Empty(t, redacted.Metering.AWS.SessionToken)
	require.Empty(t, redacted.Metering.OSS.SecretAccessKey)
	require.Equal(t, "qcs::cam::uin/1:roleName/metering", redacted.Metering.COS.AssumeRoleARN)
	require.Empty(t, redacted.Metering.COS.SecretAccessKey)
	require.Empty(t, redacted.Metering.Endpoint)
	require.Equal(t, "/var/lib/tiproxy/traffic.key", redacted.Security.EncryptionKeyPath)
	require.Equal(t, "metering-container", redacted.Metering.Bucket)

	jsonBody, err := json.Marshal(redacted)
	require.NoError(t, err)
	tomlBody, err := redacted.ToBytes()
	require.NoError(t, err)
	for _, body := range []string{string(jsonBody), string(tomlBody)} {
		require.NotContains(t, body, "account-key-secret")
		require.NotContains(t, body, "sas-secret")
		require.NotContains(t, body, "aws-secret")
		require.NotContains(t, body, "oss-secret")
		require.NotContains(t, body, "cos-secret")
		require.Contains(t, body, "metering-container")
	}

	// Re-applying the public document must not wipe secrets that were omitted.
	base := cfg.Clone()
	require.NoError(t, toml.Unmarshal(tomlBody, base))
	require.Equal(t, "sas-secret", base.Metering.Azure.SASToken)
	require.Equal(t, "cos-secret", base.Metering.COS.SecretAccessKey)
	require.Equal(t, cfg.Metering.Endpoint, base.Metering.Endpoint)

	enc := zapcore.NewMapObjectEncoder()
	zap.Any("cfg", cfg).AddTo(enc)
	logged, err := json.Marshal(enc.Fields)
	require.NoError(t, err)
	require.NotContains(t, string(logged), "sas-secret")
	require.NotContains(t, string(logged), "aws-secret")
	require.NotContains(t, string(logged), "oss-secret")
	require.NotContains(t, string(logged), "cos-secret")
	require.NotContains(t, string(logged), "account-key-secret")
	require.Contains(t, string(logged), "metering-container")
}

func TestRedactedDropsPresignedEndpoint(t *testing.T) {
	cfg := NewConfig()
	cfg.Metering.Endpoint = "https://bucket.s3.amazonaws.com/obj?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261109%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=sigsecret&X-Amz-Security-Token=tokensecret"
	redacted := cfg.Redacted()
	require.Empty(t, redacted.Metering.Endpoint)

	jsonBody, err := json.Marshal(redacted)
	require.NoError(t, err)
	tomlBody, err := redacted.ToBytes()
	require.NoError(t, err)
	for _, body := range []string{string(jsonBody), string(tomlBody)} {
		require.NotContains(t, body, "sigsecret")
		require.NotContains(t, body, "tokensecret")
		require.NotContains(t, body, "AKIAEXAMPLE")
	}

	// Writing the public document back must keep the stored endpoint.
	base := cfg.Clone()
	require.NoError(t, toml.Unmarshal(tomlBody, base))
	require.Equal(t, cfg.Metering.Endpoint, base.Metering.Endpoint)

	sigCfg := NewConfig()
	sigCfg.Metering.Endpoint = "https://account.blob.core.windows.net/container?sig=sassecret"
	require.Empty(t, sigCfg.Redacted().Metering.Endpoint)

	plain := NewConfig()
	plain.Metering.Endpoint = "https://s3.example.com"
	require.Equal(t, plain.Metering.Endpoint, plain.Redacted().Metering.Endpoint)
}
