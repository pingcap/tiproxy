// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

var dirWatcherPollInterval = time.Second * 5

// dirWatcher watches a directory for new files and notifies via callback.
type dirWatcher struct {
	lg *zap.Logger

	pathPrefix string
	storage    storeapi.Storage

	recordedDirs map[string]struct{}
	onNewDir     func(path string) error
}

// NewDirWatcher creates a new dirWatcher.
func NewDirWatcher(lg *zap.Logger, pathPrefix string, onNewDir func(filename string) error, storage storeapi.Storage) *dirWatcher {
	return &dirWatcher{
		lg:           lg,
		pathPrefix:   pathPrefix,
		storage:      storage,
		recordedDirs: make(map[string]struct{}),
		onNewDir:     onNewDir,
	}
}

// Watch starts watching the directory for new files.
func (c *dirWatcher) Watch(ctx context.Context) error {
	ticker := time.NewTicker(dirWatcherPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := c.WalkFiles(ctx)
			if err != nil {
				c.lg.Warn("dir watcher encountered error", zap.Error(err))
			}
		}
	}
}

// WalkFiles walks through the files in the directory and triggers the callback for new directories.
func (c *dirWatcher) WalkFiles(ctx context.Context) error {
	var err error
	var dirs []string

	dirs, err = c.listDirs(ctx)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		if _, exists := c.recordedDirs[dir]; exists {
			continue
		}

		c.lg.Info("found new directory", zap.String("dir", dir))
		err := c.onNewDir(dir)
		if err != nil {
			c.lg.Warn("failed to handle new directory", zap.String("dir", dir), zap.Error(err))
		} else {
			c.recordedDirs[dir] = struct{}{}
		}
	}

	return nil
}

func (c *dirWatcher) listDirs(ctx context.Context) ([]string, error) {
	// Use generic WalkDir over the configured storage and derive first-level directories
	// under the configured path prefix. This works for both local and S3-like backends.
	prefix := strings.TrimLeft(c.pathPrefix, "/")
	dirSet := make(map[string]struct{})

	err := c.storage.WalkDir(ctx, &storeapi.WalkOption{}, func(name string, size int64) error {
		rel := strings.TrimLeft(name, "/")
		if prefix != "" {
			if !strings.HasPrefix(rel, prefix) {
				return nil
			}
			rel = strings.TrimPrefix(rel, prefix)
			rel = strings.TrimLeft(rel, "/")
		}
		if rel == "" {
			return nil
		}
		// Extract first path segment after the prefix.
		first := rel
		if idx := strings.Index(first, "/"); idx >= 0 {
			first = first[:idx]
		}
		if first == "" {
			return nil
		}
		dir := prefix
		if dir != "" && !strings.HasSuffix(dir, "/") {
			dir += "/"
		}
		dir += first + "/"
		dirSet[dir] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}

	dirs := make([]string, 0, len(dirSet))
	for d := range dirSet {
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// SetDirWatcherPollIntervalForTest sets the polling interval for dirWatcher.
func SetDirWatcherPollIntervalForTest(d time.Duration) {
	dirWatcherPollInterval = d
}
