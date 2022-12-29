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

package security

import (
	"crypto/tls"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func TestCertServer(t *testing.T) {
	logger := logger.CreateLoggerForTest(t)
	tmpdir := t.TempDir()
	certPath := filepath.Join(tmpdir, "cert")
	keyPath := filepath.Join(tmpdir, "key")
	caPath := filepath.Join(tmpdir, "ca")

	require.NoError(t, CreateTLSCertificates(logger, certPath, keyPath, caPath, 0, time.Hour))

	type certCase struct {
		config.TLSConfig
		server  bool
		checker func(*testing.T, *tls.Config, *CertInfo)
		err     string
	}

	cases := []certCase{
		{
			server: true,
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				CA: caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts: true,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
				CA:   caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert:   certPath,
				Key:    keyPath,
				CA:     caPath,
				SkipCA: true,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.VerifyClientCertIfGiven, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts: true,
				CA:        caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				SkipCA: true,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				SkipCA: true,
				Cert:   certPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				CA: caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
				CA:   caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
			},
			err: "",
		},
	}

	for _, tc := range cases {
		ci, tcfg, err := NewCert(logger, tc.TLSConfig, tc.server)
		if len(tc.err) > 0 {
			require.Nil(t, ci)
			require.ErrorContains(t, err, tc.err)
		} else {
			require.NotNil(t, ci)
			require.NoError(t, err)
		}
		if tc.checker != nil {
			tc.checker(t, tcfg, ci)
		}
	}
}
