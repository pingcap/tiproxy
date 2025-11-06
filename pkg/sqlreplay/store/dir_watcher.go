// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

var dirWatcherPollInterval = time.Second * 5

// dirWatcher watches a directory for new files and notifies via callback.
type dirWatcher struct {
	lg *zap.Logger

	pathPrefix string
	storage    storage.ExternalStorage

	recordedDirs map[string]struct{}
	onNewDir     func(path string) error
}

// NewDirWatcher creates a new dirWatcher.
func NewDirWatcher(lg *zap.Logger, pathPrefix string, onNewDir func(filename string) error, storage storage.ExternalStorage) *dirWatcher {
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
	case *storage.S3Storage:
		dirs, err = c.listFilesForS3(ctx)
		if err != nil {
			return err
		}
	case *storage.LocalStorage:
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
	// Assumes that we don't have more than 1000 directories under one prefix.
	s := c.storage.(*storage.S3Storage)
	options := s.GetOptions()
	req := &s3.ListObjectsInput{
		Bucket:    aws.String(options.Bucket),
		Prefix:    aws.String(c.pathPrefix),
		MaxKeys:   aws.Int64(1000),
		Delimiter: aws.String("/"),
	}

	res, err := s.GetS3APIHandle().ListObjectsWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, dir := range res.CommonPrefixes {
		dirs = append(dirs, *dir.Prefix)
	}

	return dirs, nil
}

func (c *dirWatcher) listFilesForLocal() ([]string, error) {
	var dirs []string
	s := c.storage.(*storage.LocalStorage)
	err := filepath.WalkDir(s.Base(), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}

		if !strings.HasPrefix(path, c.pathPrefix) {
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
		dirs = append(dirs, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return dirs, nil
}
