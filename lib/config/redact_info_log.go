// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

// RedactInfoLogType controls how sensitive information is written to logs and
// traffic replay report tables.
type RedactInfoLogType int

const (
	// RedactInfoLogOff disables log redaction.
	RedactInfoLogOff RedactInfoLogType = iota
	// RedactInfoLogOn replaces sensitive information with question marks.
	RedactInfoLogOn
	// RedactInfoLogMarker encloses sensitive information in redaction markers.
	RedactInfoLogMarker
)

const invalidRedactInfoLogTypeErrMsg = `the "redact-info-log" value is invalid; it should be false, true, or a case-insensitive string of "false", "true", or "marker"`

// MarshalJSON implements json.Marshaler.
func (t RedactInfoLogType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.configValue())
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *RedactInfoLogType) UnmarshalJSON(data []byte) error {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	}
	return t.setConfigValue(value)
}

// MarshalTOML implements toml.Marshaler.
func (t RedactInfoLogType) MarshalTOML() ([]byte, error) {
	switch t {
	case RedactInfoLogOn:
		return []byte("true"), nil
	case RedactInfoLogMarker:
		return []byte(`"MARKER"`), nil
	default:
		return []byte("false"), nil
	}
}

// UnmarshalTOML implements toml.Unmarshaler.
func (t *RedactInfoLogType) UnmarshalTOML(data any) error {
	return t.setConfigValue(data)
}

func (t RedactInfoLogType) configValue() any {
	switch t {
	case RedactInfoLogOn:
		return true
	case RedactInfoLogMarker:
		return "MARKER"
	default:
		return false
	}
}

func (t RedactInfoLogType) configInfoValue() string {
	switch t {
	case RedactInfoLogOn:
		return "true"
	case RedactInfoLogMarker:
		return "marker"
	default:
		return "false"
	}
}

// SQLRedactMode returns the mode accepted by the SQL normalizer: "OFF", "ON", or "MARKER".
// An unrecognized value redacts, so a bad setting does not leave literals in the report.
func (t RedactInfoLogType) SQLRedactMode() string {
	switch t {
	case RedactInfoLogOff:
		return "OFF"
	case RedactInfoLogMarker:
		return "MARKER"
	default:
		return "ON"
	}
}

func (t *RedactInfoLogType) setConfigValue(value any) error {
	switch v := value.(type) {
	case bool:
		if v {
			*t = RedactInfoLogOn
		} else {
			*t = RedactInfoLogOff
		}
		return nil
	case string:
		switch {
		case strings.EqualFold(v, "true"):
			*t = RedactInfoLogOn
		case strings.EqualFold(v, "false"):
			*t = RedactInfoLogOff
		case strings.EqualFold(v, "marker"):
			*t = RedactInfoLogMarker
		default:
			return errors.New(invalidRedactInfoLogTypeErrMsg)
		}
		return nil
	}
	return errors.New(invalidRedactInfoLogTypeErrMsg)
}
