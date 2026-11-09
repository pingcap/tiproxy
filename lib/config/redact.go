// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"net/url"
	"slices"
	"strings"

	mconfig "github.com/pingcap/metering_sdk/config"
	"go.uber.org/zap/zapcore"
)

// secretQueryKeys are credential query parameters that may be embedded in a
// storage endpoint. Names are compared after lower-casing and replacing '_'.
var secretQueryKeys = map[string]struct{}{
	"access-key":        {},
	"secret-access-key": {},
	"session-token":     {},
	"account-key":       {},
	"sas-token":         {},
	"password":          {},
	"encryption-key":    {},
	"sig":               {},
}

// Redacted returns a copy that is safe to return from the admin API and to write
// to logs. Cloud credentials are cleared so omitempty drops them. Non-secret
// settings, including credential file paths, are preserved.
//
// The result must not replace the process configuration. A client that sends the
// redacted document back updates only the keys that are still present, so the
// live secrets are kept.
func (cfg *Config) Redacted() *Config {
	if cfg == nil {
		return nil
	}
	out := cfg.Clone()
	out.Metering = redactMetering(out.Metering)
	return out
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
// zap.Any uses it instead of reflecting every exported field into the log.
func (cfg *Config) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if cfg == nil {
		return nil
	}
	return writeLogObject(enc, cfg.Redacted())
}

func redactMetering(mc mconfig.MeteringConfig) mconfig.MeteringConfig {
	if mc.AWS != nil {
		aws := *mc.AWS
		aws.AccessKey = ""
		aws.SecretAccessKey = ""
		aws.SessionToken = ""
		mc.AWS = &aws
	}
	if mc.OSS != nil {
		oss := *mc.OSS
		oss.AccessKey = ""
		oss.SecretAccessKey = ""
		oss.SessionToken = ""
		mc.OSS = &oss
	}
	if mc.COS != nil {
		cos := *mc.COS
		cos.AccessKey = ""
		cos.SecretAccessKey = ""
		cos.SessionToken = ""
		mc.COS = &cos
	}
	if mc.Azure != nil {
		azure := *mc.Azure
		azure.AccountKey = ""
		azure.SASToken = ""
		mc.Azure = &azure
	}
	mc.Endpoint = redactEndpoint(mc.Endpoint)
	return mc
}

// redactEndpoint drops an endpoint that embeds credentials.
// Clearing it lets omitempty remove the field, so writing the public config
// back does not replace the stored endpoint. An endpoint without credentials
// is returned unchanged.
func redactEndpoint(raw string) string {
	if raw == "" || (!strings.Contains(raw, "://") && !strings.Contains(raw, "?") && !strings.Contains(raw, "@")) {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.User != nil {
		return ""
	}
	if u.RawQuery == "" {
		return raw
	}
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return ""
	}
	for key := range values {
		normalized := strings.ToLower(strings.ReplaceAll(key, "_", "-"))
		if _, ok := secretQueryKeys[normalized]; ok {
			return ""
		}
	}
	return raw
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
