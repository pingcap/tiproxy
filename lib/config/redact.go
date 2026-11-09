// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"net/url"
	"strings"

	mconfig "github.com/pingcap/metering_sdk/config"
)

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

// String returns the redacted config as JSON.
// zap.Any prefers fmt.Stringer, and the TiDB text encoder then writes that JSON
// as one escaped field. An ObjectMarshaler would be rendered as nested brackets
// inside the field, which the text format cannot parse.
func (cfg *Config) String() string {
	if cfg == nil {
		return ""
	}
	raw, err := json.Marshal(cfg.Redacted())
	if err != nil {
		return ""
	}
	return string(raw)
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

// redactEndpoint omits an endpoint that has userinfo or a query.
// An empty value is dropped by omitempty, so writing the public config back does
// not replace the stored endpoint. Returning only the prefix would overwrite it.
// A plain endpoint is returned unchanged.
func redactEndpoint(raw string) string {
	if raw == "" || (!strings.Contains(raw, "://") && !strings.Contains(raw, "?") && !strings.Contains(raw, "@")) {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ""
	}
	return raw
}
