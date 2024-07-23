// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package httputil

import (
	"context"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"net/http"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestHTTPGet(t *testing.T) {
	httpHandler := &mockHttpHandler{
		t: t,
	}
	httpHandler.setHTTPResp(true)
	httpHandler.setHTTPRespBody("hello")
	statusListener, statusAddr := testkit.StartListener(t, "")
	statusServer := &http.Server{Addr: statusAddr, Handler: httpHandler}
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		_ = statusServer.Serve(statusListener)
	})
	certManager := cert.NewCertManager()
	httpCli := NewHTTPClient(certManager.ClusterTLS)
	httpCli.SetTimeout(time.Second)
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), uint64(2)), context.Background())

	resp, err := Get(*httpCli, statusAddr, "", b)
	require.NoError(t, err)
	require.Equal(t, "hello", string(resp))

	httpHandler.setHTTPResp(false)
	_, err = Get(*httpCli, statusAddr, "", b)
	require.Error(t, err)

	_, err = Get(*httpCli, statusAddr, "", b)
	require.Error(t, err)

	_, err = Get(*httpCli, statusAddr, "", b)
	require.Error(t, err)

	httpCli.SetTimeout(time.Millisecond)
	httpHandler.setHTTPWait(100 * time.Millisecond)
	_, err = Get(*httpCli, statusAddr, "", b)
	require.Error(t, err)

	err = statusServer.Close()
	require.NoError(t, err)
	wg.Wait()

	_, err = Get(*httpCli, statusAddr, "", b)
	require.Error(t, err)
}

type mockHttpHandler struct {
	t        *testing.T
	httpOK   atomic.Bool
	respBody atomic.String
	wait     atomic.Int64
}

func (handler *mockHttpHandler) setHTTPResp(succeed bool) {
	handler.httpOK.Store(succeed)
}

func (handler *mockHttpHandler) setHTTPRespBody(body string) {
	handler.respBody.Store(body)
}

func (handler *mockHttpHandler) setHTTPWait(wait time.Duration) {
	handler.wait.Store(int64(wait))
}

func (handler *mockHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	wait := handler.wait.Load()
	if wait > 0 {
		time.Sleep(time.Duration(wait))
	}
	if handler.httpOK.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(handler.respBody.Load()))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
