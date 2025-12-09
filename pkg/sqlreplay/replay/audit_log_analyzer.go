// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"io"
	"net/url"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

// Analyze analyzes the audit log in the given input between startTime and endTime and return the analysis result
// in CSV format.
func Analyze(lg *zap.Logger, cfg cmd.AnalyzeConfig) (cmd.AuditLogAnalyzeResult, error) {
	var storages []storage.ExternalStorage
	var readers []cmd.LineReader
	defer func() {
		for _, r := range readers {
			r.Close()
		}
		for _, s := range storages {
			s.Close()
		}
	}()

	storage, err := store.NewStorage(cfg.Input)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid input %s", cfg.Input)
	}

	parsedURL, err := url.Parse(cfg.Input)
	if err != nil {
		return nil, err
	}
	watcher := store.NewDirWatcher(lg.Named("dir_watcher"), strings.TrimLeft(parsedURL.Path, "/"), func(fileName string) error {
		url, err := url.Parse(cfg.Input)
		if err != nil {
			return err
		}
		url.Path = fileName
		s, err := store.NewStorage(url.String())
		if err != nil {
			return err
		}
		storages = append(storages, s)

		cfg := store.ReaderCfg{
			Format:             cmd.FormatAuditLogPlugin,
			Dir:                fileName,
			FileNameFilterTime: cfg.Start,
		}
		reader, err := store.NewReader(lg.Named("loader"), s, cfg)
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		return nil
	}, storage)

	err = watcher.WalkFiles(context.Background())
	if err != nil {
		return nil, err
	}

	var wg waitgroup.WaitGroup
	resultCh := make(chan cmd.AuditLogAnalyzeResult, len(readers))
	for _, reader := range readers {
		analyzer := cmd.NewAuditLogAnalyzer(reader, cfg)
		wg.Run(func() {
			result, err := analyzer.Analyze()
			if err != nil && !errors.Is(err, io.EOF) {
				lg.Error("analyze audit log failed", zap.Error(err))
			}
			resultCh <- result
		}, lg)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	finalResult := make(cmd.AuditLogAnalyzeResult)
	for res := range resultCh {
		finalResult.Merge(res)
	}

	return finalResult, nil
}
