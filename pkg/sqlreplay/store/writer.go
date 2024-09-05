// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"
)

type Writer interface {
	Write([]byte) error
	Close() error
}

type WriterCfg struct {
	Dir      string
	FileSize int
}

var _ Writer = (*writer)(nil)

type writer struct {
	lg *lumberjack.Logger
}

func NewWriter(cfg WriterCfg) *writer {
	if cfg.FileSize == 0 {
		cfg.FileSize = fileSize
	}
	return &writer{
		lg: &lumberjack.Logger{
			Filename:  filepath.Join(cfg.Dir, fileName),
			MaxSize:   cfg.FileSize,
			LocalTime: true,
			Compress:  true,
		},
	}
}

func (w *writer) Write(data []byte) error {
	_, err := w.lg.Write(data)
	return err
}

func (w *writer) Close() error {
	return w.lg.Close()
}
