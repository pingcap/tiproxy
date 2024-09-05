// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileRotation(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewWriter(WriterCfg{
		Dir:      tmpDir,
		FileSize: 1,
	})
	defer writer.Close()

	data := make([]byte, 100*1024)
	for i := 0; i < 11; i++ {
		require.NoError(t, writer.Write(data))
	}

	// files are rotated and compressed at backendground asynchronously
	require.Eventually(t, func() bool {
		files := listFiles(t, tmpDir)
		count := 0
		compressed := false
		for _, f := range files {
			if strings.HasPrefix(f, "traffic") {
				count++
			}
			if strings.HasSuffix(f, ".gz") {
				compressed = true
			}
		}
		if count == 2 && compressed {
			return true
		}
		t.Logf("traffic files: %v", files)
		return false
	}, 5*time.Second, 10*time.Millisecond)
}

func listFiles(t *testing.T, dir string) []string {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, f := range files {
		names = append(names, f.Name())
	}
	return names
}
