// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	mgrcrt "github.com/pingcap/tiproxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/tiproxy/pkg/manager/config"
	mgrns "github.com/pingcap/tiproxy/pkg/manager/namespace"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func createServer(t *testing.T, closing *bool) (*Server, func(t *testing.T, method string, path string, rd io.Reader, f func(*testing.T, *http.Response))) {
	lg, _ := logger.CreateLoggerForTest(t)
	ready := atomic.NewBool(true)
	cfgmgr := mgrcfg.NewConfigManager()
	if closing == nil {
		g := false
		closing = &g
	}
	require.NoError(t, cfgmgr.Init(context.Background(), lg, "", nil))
	crtmgr := mgrcrt.NewCertManager()
	crtmgr.Init(cfgmgr.GetConfig(), lg, cfgmgr.WatchConfig())
	srv, err := NewServer(config.API{
		Addr: "0.0.0.0:0",
	}, lg, func() bool {
		return *closing
	}, mgrns.NewNamespaceManager(), cfgmgr, crtmgr, nil, ready)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
	})

	addr := fmt.Sprintf("http://%s", srv.listener.Addr().String())
	return srv, func(t *testing.T, method, pa string, rd io.Reader, f func(*testing.T, *http.Response)) {
		if pa[0] != '/' {
			pa = "/" + pa
		}
		req, err := http.NewRequest(method, fmt.Sprintf("%s%s", addr, pa), rd)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		f(t, resp)
		require.NoError(t, resp.Body.Close())
	}
}

func TestGrpc(t *testing.T) {
	srv, _ := createServer(t, nil)
	addr := srv.listener.Addr().String()
	cc, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	require.NoError(t, cc.Close())
}
