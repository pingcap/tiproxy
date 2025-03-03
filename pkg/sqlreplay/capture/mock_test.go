// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"bytes"
	"io"
	"sync"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
)

var _ io.WriteCloser = (*mockWriter)(nil)

type mockWriter struct {
	sync.Mutex
	buf bytes.Buffer
}

func newMockWriter(store.WriterCfg) *mockWriter {
	return &mockWriter{}
}

func (w *mockWriter) Write(p []byte) (int, error) {
	w.Lock()
	defer w.Unlock()
	return w.buf.Write(p)
}

func (w *mockWriter) getData() []byte {
	w.Lock()
	defer w.Unlock()
	return w.buf.Bytes()
}

func (w *mockWriter) Close() error {
	return nil
}

func mockInitSession() (string, error) {
	return "init session", nil
}
