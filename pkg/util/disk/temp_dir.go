// Copyright 2020 Ipalfish, Inc.
// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disk

import (
	"os"
	"path/filepath"

	"github.com/danjacques/gofslock/fslock"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	lockFile = "_dir.lock"
)

// InitializeTempDir initializes the temp directory.
func InitializeTempDir(tempDir string) error {
	_, err := os.Stat(tempDir)
	if err != nil && !os.IsExist(err) {
		err = os.MkdirAll(tempDir, 0750)
		if err != nil {
			return err
		}
	}
	_, err = fslock.Lock(filepath.Join(tempDir, lockFile))
	if err != nil {
		switch err {
		case fslock.ErrLockHeld:
			logutil.BgLogger().Error("The current temporary storage dir has been occupied by another instance, "+
				"check tmp-storage-path config and make sure they are different.", zap.String("TempStoragePath", tempDir), zap.Error(err))
		default:
			logutil.BgLogger().Error("Failed to acquire exclusive lock on the temporary storage dir.", zap.String("TempStoragePath", tempDir), zap.Error(err))
		}
		return err
	}

	subDirs, err := os.ReadDir(tempDir)
	if err != nil {
		return err
	}

	// If it exists others files except lock file, creates another goroutine to clean them.
	if len(subDirs) > 2 {
		go func() {
			for _, subDir := range subDirs {
				// Do not remove the lock file.
				switch subDir.Name() {
				case lockFile:
					continue
				}
				err := os.RemoveAll(filepath.Join(tempDir, subDir.Name()))
				if err != nil {
					logutil.BgLogger().Warn("Remove temporary file error",
						zap.String("tempStorageSubDir", filepath.Join(tempDir, subDir.Name())), zap.Error(err))
				}
			}
		}()
	}
	return nil
}
