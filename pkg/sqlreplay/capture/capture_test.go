// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"bufio"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestStartAndStop(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	tmpDir := t.TempDir()
	fileName := filepath.Join(tmpDir, "traffic.log")
	cpt := NewCapture(lg)
	defer cpt.Close()

	packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	cpt.Capture(packet, time.Now(), 100)
	cfg := CaptureConfig{Filename: fileName, Duration: 10 * time.Second}

	// start capture and the traffic should be outputted
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(packet, time.Now(), 100)
	cpt.Stop(nil)
	cpt.wg.Wait()
	fileSize := getFileSize(t, fileName)
	require.Greater(t, fileSize, int64(0))
	data := readFile(t, fileName)
	require.Contains(t, string(data), "select 1")

	// stop capture and traffic should not be outputted
	cpt.Capture(packet, time.Now(), 100)
	cpt.wg.Wait()
	require.Equal(t, fileSize, getFileSize(t, fileName))

	// start capture again
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(packet, time.Now(), 100)
	cpt.Stop(nil)
	cpt.wg.Wait()
	require.Greater(t, getFileSize(t, fileName), fileSize)

	// duplicated start and stop
	require.NoError(t, cpt.Start(cfg))
	require.Error(t, cpt.Start(cfg))
	cpt.Stop(nil)
	cpt.Stop(nil)
}

func TestFileRotation(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	tmpDir := t.TempDir()
	fileName := filepath.Join(tmpDir, "traffic.log")
	cpt := NewCapture(lg)
	defer cpt.Close()

	packet := make([]byte, 100*1024)
	packet[0] = pnet.ComQuery.Byte()
	cfg := CaptureConfig{
		Filename:       fileName,
		Duration:       10 * time.Second,
		fileSize:       1,
		bufferCap:      120 * 1024,
		flushThreshold: 80 * 1024,
	}

	require.NoError(t, cpt.Start(cfg))
	for i := 0; i < 11; i++ {
		cpt.Capture(packet, time.Now(), 100)
	}
	cpt.Stop(nil)
	cpt.wg.Wait()

	// files are rotated and compressed
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

func TestConcurrency(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	tmpDir := t.TempDir()
	fileName := filepath.Join(tmpDir, "traffic.log")
	cpt := NewCapture(lg)
	defer cpt.Close()

	cfg := CaptureConfig{
		Filename:       fileName,
		Duration:       10 * time.Second,
		fileSize:       1,
		bufferCap:      12 * 1024,
		flushThreshold: 8 * 1024,
	}
	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	wg.Run(func() {
		packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Microsecond):
				cpt.Capture(packet, time.Now(), 100)
			}
		}
	})
	wg.Run(func() {
		started := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				if started {
					cpt.Stop(nil)
					started = false
				} else {
					require.NoError(t, cpt.Start(cfg))
					started = true
				}
			}
		}
	})
	wg.Wait()
	cancel()

	files := listFiles(t, tmpDir)
	require.GreaterOrEqual(t, len(files), 1)
}

func TestCaptureCfgError(t *testing.T) {
	cfgs := []CaptureConfig{
		{
			Duration: 10 * time.Second,
		},
		{
			Filename: "traffic.log",
		},
		{
			Filename: t.TempDir(),
		},
	}

	for _, cfg := range cfgs {
		err := checkCaptureConfig(&cfg)
		require.Error(t, err)
	}
}

func readFile(t *testing.T, fileName string) []byte {
	file, err := os.Open(fileName)
	require.NoError(t, err)

	var reader *bufio.Reader
	if strings.HasSuffix(fileName, ".gz") {
		gr, err := gzip.NewReader(file)
		require.NoError(t, err)
		reader = bufio.NewReader(gr)
	} else {
		reader = bufio.NewReader(file)
	}

	p := make([]byte, 1024)
	n, err := reader.Read(p)
	require.NoError(t, err)
	return p[:n]
}

func getFileSize(t *testing.T, fileName string) int64 {
	file, err := os.Stat(fileName)
	require.NoError(t, err)
	return file.Size()
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
