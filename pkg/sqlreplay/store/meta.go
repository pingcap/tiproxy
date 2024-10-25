// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

const (
	metaFile = "meta"
	version  = "v1"
)

type Meta struct {
	Version      string
	Duration     time.Duration
	Cmds         uint64
	FilteredCmds uint64
}

func NewMeta(duration time.Duration, cmds, filteredCmds uint64) *Meta {
	return &Meta{
		Version:      version,
		Duration:     duration,
		Cmds:         cmds,
		FilteredCmds: filteredCmds,
	}
}

func (m *Meta) Write(path string) error {
	filePath := filepath.Join(path, metaFile)
	b, err := json.Marshal(m)
	if err != nil {
		return errors.WithStack(err)
	}
	if err = os.WriteFile(filePath, b, 0600); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *Meta) Read(path string) error {
	filePath := filepath.Join(path, metaFile)
	b, err := os.ReadFile(filePath)
	if err != nil {
		return errors.WithStack(err)
	}
	if err = json.Unmarshal(b, m); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
