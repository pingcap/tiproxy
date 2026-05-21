// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	mgrcrt "github.com/pingcap/tiproxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/tiproxy/pkg/manager/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type httpOpts struct {
	reader io.Reader
	header map[string]string
}

type doHTTPFunc func(t *testing.T, method string, path string, opts httpOpts, f func(*testing.T, *http.Response))

func createServer(t *testing.T) (*Server, doHTTPFunc) {
	srv, doHTTP, _ := createServerWithConfig(t, "")
	return srv, doHTTP
}

func createServerWithConfig(t *testing.T, tomlConfig string) (*Server, doHTTPFunc, doHTTPFunc) {
	lg, _ := logger.CreateLoggerForTest(t)
	ready := atomic.NewBool(true)
	cfgmgr := mgrcfg.NewConfigManager()
	require.NoError(t, cfgmgr.Init(context.Background(), "", ""))
	if tomlConfig != "" {
		require.NoError(t, cfgmgr.SetTOMLConfig([]byte(tomlConfig)))
	}
	crtmgr := mgrcrt.NewCertManager()
	require.NoError(t, crtmgr.Init(cfgmgr.GetConfig(), lg, cfgmgr.WatchConfig()))
	nsMgr := newMockNamespaceManager()
	srv, err := NewServer(config.API{
		Addr: "0.0.0.0:0",
	}, lg, Managers{
		CfgMgr:        cfgmgr,
		NsMgr:         nsMgr,
		CertMgr:       crtmgr,
		BackendReader: &mockBackendReader{},
		ReplayJobMgr:  &mockReplayJobManager{},
	}, nil, ready)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
	})

	addr := srv.listener.Addr().String()
	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	do := func(scheme string, client *http.Client) doHTTPFunc {
		return func(t *testing.T, method, pa string, opts httpOpts, f func(*testing.T, *http.Response)) {
			if pa[0] != '/' {
				pa = "/" + pa
			}
			req, err := http.NewRequest(method, fmt.Sprintf("%s://%s%s", scheme, addr, pa), opts.reader)
			require.NoError(t, err)
			for key, value := range opts.header {
				req.Header.Set(key, value)
			}
			resp, err := client.Do(req)
			require.NoError(t, err)
			f(t, resp)
			require.NoError(t, resp.Body.Close())
		}
	}
	return srv, do("http", http.DefaultClient), do("https", httpsClient)
}

func TestGrpc(t *testing.T) {
	srv, _ := createServer(t)
	addr := srv.listener.Addr().String()
	cc, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	require.NoError(t, cc.Close())
}
