// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tiproxy/lib/util/errors"
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

	switch c.storage.(type) {
	case *s3like.Storage:
		dirs, err = c.listFilesForS3(ctx)
		if err != nil {
			return err
		}
	case *objstore.LocalStorage:
		dirs, err = c.listFilesForLocal()
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported storage type for dir watcher")
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

func (c *dirWatcher) listFilesForS3(ctx context.Context) ([]string, error) {
	s, ok := c.storage.(*s3like.Storage)
	if !ok {
		return nil, errors.New("not s3 storage")
	}
	dirs := make(map[string]struct{})
	err := s.WalkDir(ctx, &storeapi.WalkOption{ObjPrefix: c.pathPrefix}, func(name string, size int64) error {
		if !strings.HasPrefix(name, c.pathPrefix) {
			return nil
		}
		rest := strings.TrimPrefix(name, c.pathPrefix)
		if rest == "" {
			return nil
		}
		if idx := strings.Index(rest, "/"); idx >= 0 {
			dirs[c.pathPrefix+rest[:idx+1]] = struct{}{}
			return nil
		}
		if strings.HasSuffix(name, "/") || size <= 0 {
			dirs[name] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0, len(dirs))
	for dir := range dirs {
		ret = append(ret, dir)
	}
	return ret, nil
}

func (c *dirWatcher) listFilesForLocal() ([]string, error) {
	var dirs []string
	s := c.storage.(*objstore.LocalStorage)
	err := filepath.WalkDir(s.Base(), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}

		// For local storage, the `path` has '/' prefix. However, the pathPrefix may not
		// have '/' prefix. So we trim the left '/' from path before comparison.
		if !strings.HasPrefix(strings.TrimLeft(path, "/"), strings.TrimLeft(c.pathPrefix, "/")) {
			return nil
		}
		relPath, err := filepath.Rel(s.Base(), path)
		if err != nil {
			return err
		}
		if strings.Contains(relPath, string(os.PathSeparator)) && relPath != "." {
			// Only watch first-level directories under the prefix.
			return filepath.SkipDir
		}
		// Add trailing slash to indicate it's a directory and keep consistent with S3 storage.
		if !strings.HasSuffix(path, string(os.PathSeparator)) {
			path += string(os.PathSeparator)
		}
		dirs = append(dirs, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return dirs, nil
}

// SetDirWatcherPollIntervalForTest sets the polling interval for dirWatcher.
func SetDirWatcherPollIntervalForTest(d time.Duration) {
	dirWatcherPollInterval = d
}
