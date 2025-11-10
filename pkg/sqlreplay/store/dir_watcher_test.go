// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestLocalDirWatcher(t *testing.T) {
	tempDir := t.TempDir()
	pathPrefix := filepath.Join(tempDir, "dir_watcher_test_")
	backend, err := storage.ParseBackend(tempDir, &storage.BackendOptions{})
	require.NoError(t, err)
	s, err := storage.New(context.Background(), backend, nil)
	require.NoError(t, err)

	dirWatcherPollInterval = time.Millisecond * 10

	// Create two new directory
	newDir := filepath.Join(tempDir, "dir_watcher_test_dir1")
	require.NoError(t, os.MkdirAll(newDir, 0o755))
	defer os.RemoveAll(newDir)
	newDir2 := filepath.Join(tempDir, "dir_watcher_test_dir2")
	require.NoError(t, os.MkdirAll(newDir2, 0o755))
	defer os.RemoveAll(newDir2)

	// Create a new directory inside the new directory
	newSubDir := filepath.Join(newDir, "subdir")
	require.NoError(t, os.MkdirAll(newSubDir, 0o755))
	defer os.RemoveAll(newSubDir)

	// Create a new directory with wrong prefix
	wrongDir := filepath.Join(tempDir, "wrong_prefix_dir")
	require.NoError(t, os.MkdirAll(wrongDir, 0o755))
	defer os.RemoveAll(wrongDir)

	// Create a file in the tempdir
	wrongFile := filepath.Join(wrongDir, "dir_watcher_test_file.txt")
	f2, err := os.Create(wrongFile)
	require.NoError(t, err)
	f2.Close()
	defer os.RemoveAll(wrongFile)

	logger := zap.NewNop()
	t.Run("WalkFiles will call callbacks on dir", func(t *testing.T) {
		files := make(map[string]struct{})
		w := NewDirWatcher(logger, pathPrefix, func(filename string) error {
			files[strings.TrimRight(filename, "/")] = struct{}{}
			return nil
		}, s)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.NoError(t, w.WalkFiles(ctx))
		require.Equal(t, map[string]struct{}{
			newDir:  {},
			newDir2: {},
		}, files)
	})

	t.Run("Watch function should have the correct result", func(t *testing.T) {
		files := make(map[string]struct{})
		w := NewDirWatcher(logger, pathPrefix, func(filename string) error {
			files[strings.TrimRight(filename, "/")] = struct{}{}
			return nil
		}, s)
		ctx, cancel := context.WithCancel(context.Background())

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, w.Watch(ctx))
		}()

		require.Eventually(t, func() bool {
			_, ok1 := files[newDir]
			_, ok2 := files[newDir2]
			return ok1 && ok2 && len(files) == 2
		}, dirWatcherPollInterval*3, time.Millisecond)

		// Add a new directory
		newDir3 := filepath.Join(tempDir, "dir_watcher_test_dir3")
		require.NoError(t, os.MkdirAll(newDir3, 0o755))
		defer os.RemoveAll(newDir3)

		require.Eventually(t, func() bool {
			_, ok := files[newDir3]
			return ok && len(files) == 3
		}, dirWatcherPollInterval*3, time.Millisecond)

		cancel()
		wg.Wait()
	})
}

func TestS3DirWatcher(t *testing.T) {
	controller := gomock.NewController(t)
	s3api := mock.NewMockS3API(controller)
	currentFiles := atomic.Pointer[[]string]{}

	dirWatcherPollInterval = time.Millisecond * 10

	s3api.EXPECT().ListObjectsWithContext(gomock.Any(), gomock.Any()).MaxTimes(4).DoAndReturn(
		func(ctx context.Context, req *s3.ListObjectsInput, _ ...request.Option) (*s3.ListObjectsOutput, error) {
			var retFiles []*s3.CommonPrefix
			files := currentFiles.Load()
			if files != nil {
				for _, f := range *files {
					if !strings.HasPrefix(f, *req.Prefix) {
						continue
					}
					if !strings.HasSuffix(f, "/") {
						continue
					}
					if strings.Count(f, "/")-strings.Count(*req.Prefix, "/") != 1 {
						continue
					}

					retFiles = append(retFiles, &s3.CommonPrefix{
						Prefix: aws.String(f),
					})
				}
			}

			return &s3.ListObjectsOutput{
				CommonPrefixes: retFiles,
			}, nil
		},
	)

	newDir := "dir_watcher_test_dir1/"
	newDir2 := "dir_watcher_test_dir2/"
	currentFiles.Store(&[]string{
		newDir,
		newDir2,
		"dir_watcher_test_dir1/subdir/",
		"dir_watcher_test_dir1/subfile",
		"wrong_prefix_dir/",
		"dir_watcher_test_file",
	})

	s := storage.NewS3StorageForTest(s3api, &backup.S3{
		Bucket: "test-bucket",
	})

	logger := zap.NewNop()
	t.Run("WalkFiles will call callbacks on dir", func(t *testing.T) {
		files := make(map[string]struct{})
		w := NewDirWatcher(logger, "dir_watcher_test_", func(filename string) error {
			files[filename] = struct{}{}
			return nil
		}, s)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.NoError(t, w.WalkFiles(ctx))
		require.Equal(t, map[string]struct{}{
			"dir_watcher_test_dir1/": {},
			"dir_watcher_test_dir2/": {},
		}, files)
	})

	t.Run("Watch function should have the correct result", func(t *testing.T) {
		files := make(map[string]struct{})
		w := NewDirWatcher(logger, "dir_watcher_test_", func(filename string) error {
			files[filename] = struct{}{}
			return nil
		}, s)
		ctx, cancel := context.WithCancel(context.Background())

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, w.Watch(ctx))
		}()

		require.Eventually(t, func() bool {
			_, ok1 := files[newDir]
			_, ok2 := files[newDir2]
			return ok1 && ok2 && len(files) == 2
		}, dirWatcherPollInterval*3, time.Millisecond)

		oldFiles := currentFiles.Load()
		newFiles := slices.Clone(*oldFiles)
		newDir3 := "dir_watcher_test_dir3/"
		newFiles = append(newFiles, newDir3)
		currentFiles.Store(&newFiles)

		require.Eventually(t, func() bool {
			_, ok := files[newDir3]
			return ok && len(files) == 3
		}, dirWatcherPollInterval*3, time.Millisecond)

		cancel()
		wg.Wait()
	})
}

func TestS3DirWatcherWithRealS3(t *testing.T) {
	// To run this test, please set the S3_URL_FOR_TEST environment variable.
	// Example: s3://test-bucket?force-path-style=true&endpoint=http://127.0.0.1:9000&access-key=minioadmin&secret-access-key=minioadmin&provider=minio
	// Or any valid S3 URL.
	url, ok := os.LookupEnv("S3_URL_FOR_TEST")
	if !ok {
		t.Skip("S3_URL_FOR_TEST not set, skipping real S3 test")
	}

	backend, err := storage.ParseBackend(url, &storage.BackendOptions{})
	require.NoError(t, err)
	s, err := storage.New(context.Background(), backend, nil)
	require.NoError(t, err)

	dirWatcherPollInterval = time.Millisecond * 10

	// Create two new directories
	require.NoError(t, s.WriteFile(context.Background(), "dir_watcher_test_dir1/.keep", []byte{1}))
	defer func() {
		require.NoError(t, s.DeleteFile(context.Background(), "dir_watcher_test_dir1/.keep"))
	}()
	require.NoError(t, s.WriteFile(context.Background(), "dir_watcher_test_dir2/.keep", []byte{1}))
	defer func() {
		require.NoError(t, s.DeleteFile(context.Background(), "dir_watcher_test_dir2/.keep"))
	}()

	// Create a new directory inside the new directory
	require.NoError(t, s.WriteFile(context.Background(), "dir_watcher_test_dir2/subdir/.keep", []byte{1}))
	defer func() {
		require.NoError(t, s.DeleteFile(context.Background(), "dir_watcher_test_dir2/subdir/.keep"))
	}()

	// Create a new directory with wrong prefix
	require.NoError(t, s.WriteFile(context.Background(), "wrong_prefix_dir/.keep", []byte{1}))
	defer func() {
		require.NoError(t, s.DeleteFile(context.Background(), "wrong_prefix_dir/.keep"))
	}()

	// Create a file in the tempdir
	require.NoError(t, s.WriteFile(context.Background(), "dir_watcher_test_file.txt", []byte{1}))
	defer func() {
		require.NoError(t, s.DeleteFile(context.Background(), "dir_watcher_test_file.txt"))
	}()

	logger := zap.NewNop()
	t.Run("WalkFiles will call callbacks on dir", func(t *testing.T) {
		files := make(map[string]struct{})
		w := NewDirWatcher(logger, "dir_watcher_test_", func(filename string) error {
			files[filename] = struct{}{}
			return nil
		}, s)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.NoError(t, w.WalkFiles(ctx))
		require.Equal(t, map[string]struct{}{
			"dir_watcher_test_dir1/": {},
			"dir_watcher_test_dir2/": {},
		}, files)
	})

	t.Run("Watch function should have the correct result", func(t *testing.T) {
		files := make(map[string]struct{})
		w := NewDirWatcher(logger, "dir_watcher_test_", func(filename string) error {
			files[filename] = struct{}{}
			return nil
		}, s)
		ctx, cancel := context.WithCancel(context.Background())

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, w.Watch(ctx))
		}()

		require.Eventually(t, func() bool {
			_, ok1 := files["dir_watcher_test_dir1/"]
			_, ok2 := files["dir_watcher_test_dir2/"]
			return ok1 && ok2 && len(files) == 2
		}, dirWatcherPollInterval*3, time.Millisecond)

		// Add a new directory
		require.NoError(t, s.WriteFile(context.Background(), "dir_watcher_test_dir3/.keep", []byte{1}))
		defer func() {
			require.NoError(t, s.DeleteFile(context.Background(), "dir_watcher_test_dir3/.keep"))
		}()

		require.Eventually(t, func() bool {
			_, ok := files["dir_watcher_test_dir3/"]
			return ok && len(files) == 3
		}, dirWatcherPollInterval*3, time.Millisecond)

		cancel()
		wg.Wait()
	})

	t.Run("Watch Function works well even if there are 2K new files in one folder", func(t *testing.T) {
		files := make(map[string]struct{})
		w := NewDirWatcher(logger, "dir_watcher_test_", func(filename string) error {
			files[filename] = struct{}{}
			return nil
		}, s)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, w.Watch(ctx))
		}()

		require.Eventually(t, func() bool {
			_, ok1 := files["dir_watcher_test_dir1/"]
			_, ok2 := files["dir_watcher_test_dir2/"]
			return ok1 && ok2 && len(files) == 2
		}, dirWatcherPollInterval*3, time.Millisecond)

		// Create 2K new files
		for i := range 2000 {
			require.NoError(t, s.WriteFile(context.Background(), fmt.Sprintf("dir_watcher_test_dir1/file_%d.txt", i), []byte{1}))
		}

		// Add a new directory
		require.NoError(t, s.WriteFile(context.Background(), "dir_watcher_test_dir3/.keep", []byte{1}))
		defer func() {
			require.NoError(t, s.DeleteFile(context.Background(), "dir_watcher_test_dir3/.keep"))
		}()

		require.Eventually(t, func() bool {
			_, ok := files["dir_watcher_test_dir3/"]
			return ok && len(files) == 3
		}, dirWatcherPollInterval*3, time.Millisecond)

		cancel()
		wg.Wait()
	})
}
