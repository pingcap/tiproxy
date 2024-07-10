// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package etcd

import (
	"context"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/stretchr/testify/require"
)

func TestEtcdClient(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	server, err := CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	endpoint := server.Clients[0].Addr().String()

	cfg := ConfigForEtcdTest(endpoint)
	certMgr := cert.NewCertManager()
	err = certMgr.Init(cfg, lg, nil)
	require.NoError(t, err)
	client, err := InitEtcdClient(lg, cfg, certMgr)
	require.NoError(t, err)

	_, err = client.Put(context.Background(), "key", "value")
	require.NoError(t, err)
	resp, err := client.Get(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, "value", string(resp.Kvs[0].Value))

	require.NoError(t, client.Close())
	server.Close()
}
