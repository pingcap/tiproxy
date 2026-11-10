// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package controlbridge

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	controlpb "github.com/pingcap/tiproxy/pkg/controlbridge/pb"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestSnapshotBuilderBuildsCompleteDeterministicSnapshot(t *testing.T) {
	tlsRoot := t.TempDir()
	certPath := writeSnapshotTestFile(t, tlsRoot, "frontend.crt")
	keyPath := writeSnapshotTestFile(t, tlsRoot, "frontend.key")
	caPath := writeSnapshotTestFile(t, tlsRoot, "ca.crt")
	cfg := config.NewConfig()
	cfg.Proxy.Addr = "127.0.0.1:6000"
	cfg.Proxy.PortRange = []int{6000, 6001}
	cfg.Proxy.MaxConnections = 123
	cfg.Proxy.ConnBufferSize = 0
	cfg.Proxy.ProxyProtocol = "v2"
	cfg.Proxy.GracefulWaitBeforeShutdown = 7
	cfg.Proxy.GracefulCloseConnTimeout = 11
	cfg.Proxy.PublicEndpoints = []string{"2001:db8::1", "10.0.0.9", "10.0.0.0/24", "10.0.0.0/24"}
	cfg.Security.RequireBackendTLS = true
	cfg.Security.ServerSQLTLS = config.TLSConfig{
		Cert:          certPath,
		Key:           keyPath,
		CA:            caPath,
		MinTLSVersion: "1.3",
		CertAllowedCN: []string{"client-b", " client-a ", "client-b"},
	}
	cfg.Security.SQLTLS = config.TLSConfig{CA: caPath, MinTLSVersion: "1.2"}
	cfg.EnableTrafficReplay = false

	builder, err := NewSnapshotBuilder(cfg, []string{tlsRoot})
	require.NoError(t, err)
	backends := []*controlpb.BackendSnapshot{{BackendId: "b"}, {BackendId: "a"}}
	namespaces := []*controlpb.NamespaceSnapshot{{Name: "z"}, {Name: "default"}}
	envelope, err := builder.Build(9, cfg, 0xaabb, " TiProxy-test ", backends, namespaces)
	require.NoError(t, err)

	require.Equal(t, uint64(9), envelope.GetGeneration())
	require.Equal(t, controlpb.Priority_PRIORITY_CONTROL, envelope.GetPriority())
	snapshot := envelope.GetStateSnapshot()
	require.NotNil(t, snapshot)
	actual := snapshot.GetConfig()
	require.Equal(t, uint64(123), actual.GetMaxConnections())
	require.Equal(t, uint32(pnet.DefaultConnBufferSize), actual.GetConnectionBufferBytes())
	require.Equal(t, controlpb.ProxyProtocolMode_PROXY_PROTOCOL_MODE_V2, actual.GetProxyProtocol())
	require.Equal(t, uint64(7000), actual.GetGracefulWaitMillis())
	require.Equal(t, uint64(11000), actual.GetGracefulCloseMillis())
	require.Equal(t, []string{"2001:db8::/32", "10.0.0.9/32", "10.0.0.0/24", "10.0.0.0/24"}, actual.GetPublicCidrs())
	require.Equal(t, uint32(0xaabb), actual.GetAdvertisedCapability())
	require.Equal(t, "TiProxy-test", actual.GetServerVersion())
	require.Equal(t, []string{"client-a", "client-b"}, actual.GetFrontendTls().GetAllowedCommonNames())
	require.Equal(t, []string{"sql-0", "sql-1"}, []string{actual.GetListeners()[0].GetName(), actual.GetListeners()[1].GetName()})
	require.Equal(t, []string{"a", "b"}, []string{snapshot.GetBackends()[0].GetBackendId(), snapshot.GetBackends()[1].GetBackendId()})
	require.Equal(t, []string{"default", "z"}, []string{snapshot.GetNamespaces()[0].GetName(), snapshot.GetNamespaces()[1].GetName()})

	backends[0].BackendId = "changed"
	namespaces[0].Name = "changed"
	require.Equal(t, "b", snapshot.GetBackends()[1].GetBackendId())
	require.Equal(t, "z", snapshot.GetNamespaces()[1].GetName())
}

func TestSnapshotBuilderRejectsUnsafeOrRestartRequiredValues(t *testing.T) {
	tlsRoot := t.TempDir()
	outside := writeSnapshotTestFile(t, t.TempDir(), "outside.crt")
	cfg := config.NewConfig()
	cfg.Proxy.Addr = "127.0.0.1:6000"
	cfg.EnableTrafficReplay = false
	builder, err := NewSnapshotBuilder(cfg, []string{tlsRoot})
	require.NoError(t, err)

	_, err = builder.Build(0, cfg, 0, "test", nil, nil)
	require.ErrorContains(t, err, "generation")

	changed := cfg.Clone()
	changed.Proxy.Addr = "127.0.0.1:6001"
	_, err = builder.Build(1, changed, 0, "test", nil, nil)
	require.ErrorContains(t, err, "restart-required")

	invalidKeepalive := cfg.Clone()
	invalidKeepalive.Proxy.FrontendKeepalive.Idle = -time.Second
	invalidKeepalive.Security.ServerSQLTLS.MinTLSVersion = "1.0"
	envelope, err := builder.Build(1, invalidKeepalive, 0, "test", nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), envelope.GetStateSnapshot().GetConfig().GetFrontendKeepalive().GetIdleMillis())
	require.Equal(t, "1.0", envelope.GetStateSnapshot().GetConfig().GetFrontendTls().GetMinimumVersion())

	partialTLS := cfg.Clone()
	partialTLS.Security.ServerSQLTLS.Cert = outside
	_, err = builder.Build(1, partialTLS, 0, "test", nil, nil)
	require.ErrorContains(t, err, "outside configured TLS roots")

	unsafeTLS := cfg.Clone()
	unsafeTLS.Security.ServerSQLTLS.Cert = outside
	unsafeTLS.Security.ServerSQLTLS.Key = outside
	_, err = builder.Build(1, unsafeTLS, 0, "test", nil, nil)
	require.ErrorContains(t, err, "outside configured TLS roots")

	autoTLS := cfg.Clone()
	autoTLS.Security.ServerSQLTLS.AutoCerts = true
	_, err = builder.Build(1, autoTLS, 0, "test", nil, nil)
	require.ErrorContains(t, err, "auto-certs is unsupported")
}

func TestSnapshotBuilderRejectsInvalidAllowlistAndSubMillisecondKeepalive(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Proxy.Addr = "127.0.0.1:6000"
	_, err := NewSnapshotBuilder(cfg, []string{"relative"})
	require.ErrorContains(t, err, "must be absolute")

	builder, err := NewSnapshotBuilder(cfg, []string{t.TempDir()})
	require.NoError(t, err)
	cfg.Proxy.FrontendKeepalive.Idle = time.Microsecond
	cfg.Security.ServerSQLTLS.MinTLSVersion = "nope"
	envelope, err := builder.Build(1, cfg, 0, "test", nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), envelope.GetStateSnapshot().GetConfig().GetFrontendKeepalive().GetIdleMillis())
	require.Equal(t, "1.2", envelope.GetStateSnapshot().GetConfig().GetFrontendTls().GetMinimumVersion())
}

func writeSnapshotTestFile(t *testing.T, directory, name string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	require.NoError(t, os.WriteFile(path, []byte("fixture"), 0o600))
	return path
}
