// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
)

const (
	metaFile = "meta"
	version  = "v1"
)

type Meta struct {
	Version       string
	Duration      time.Duration
	Cmds          uint64
	FilteredCmds  uint64 `json:"FilteredCmds,omitempty"`
	EncryptMethod string `json:"EncryptMethod,omitempty"`
}

func NewMeta(duration time.Duration, cmds, filteredCmds uint64, EncryptMethod string) *Meta {
	return &Meta{
		Version:       version,
		Duration:      duration,
		Cmds:          cmds,
		FilteredCmds:  filteredCmds,
		EncryptMethod: EncryptMethod,
	}
}

func (m *Meta) Write(externalStorage storage.ExternalStorage) error {
	b, err := json.Marshal(m)
	if err != nil {
		return errors.WithStack(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err = externalStorage.WriteFile(ctx, metaFile, b); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *Meta) Read(externalStorage storage.ExternalStorage) error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	b, err := externalStorage.ReadFile(ctx, metaFile)
	if err != nil {
		return errors.WithStack(err)
	}
	if err = json.Unmarshal(b, m); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func PreCheckMeta(externalStorage storage.ExternalStorage) error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	exists, err := externalStorage.FileExists(ctx, metaFile)
	cancel()
	if err != nil {
		return errors.Wrapf(err, "check meta file failed")
	}
	if exists {
		return errors.Errorf("file %s already exists, please remove it before capture", metaFile)
	}
	return nil
}
