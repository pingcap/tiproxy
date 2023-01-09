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

package cli

import (
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/cmd"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/spf13/cobra"
)

func GetRootCmd(tlsConfig *tls.Config) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "tiproxyctl",
		Short:        "cli",
		SilenceUsage: true,
	}
	rootCmd.SetOutput(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	ctx := &Context{}

	curls := rootCmd.PersistentFlags().StringArray("curls", []string{"localhost:3080"}, "API gateway addresses")
	logEncoder := rootCmd.PersistentFlags().String("log_encoder", "tidb", "log in format of tidb, console, or json")
	logLevel := rootCmd.PersistentFlags().String("log_level", "warn", "log level")
	insecure := rootCmd.PersistentFlags().BoolP("insecure", "k", false, "enable TLS without CA, useful for testing, or for expired certs")
	caPath := rootCmd.PersistentFlags().String("ca", "", "CA to verify server certificates, set to 'skip' if want to enable SSL without verification")
	certPath := rootCmd.PersistentFlags().String("cert", "", "cert for server-side client authentication")
	keyPath := rootCmd.PersistentFlags().String("key", "", "key for server-side client authentication")
	rootCmd.PersistentFlags().Bool("indent", true, "whether indent the returned json")
	rootCmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		logger, _, _, err := cmd.BuildLogger(&config.Log{
			Encoder: *logEncoder,
			LogOnline: config.LogOnline{
				Level: *logLevel,
			},
		})
		if err != nil {
			return err
		}
		if tlsConfig == nil {
			skipCA := *insecure
			realCAPath := *caPath
			if skipCA {
				realCAPath = ""
			}
			tlsConfig, err = security.BuildClientTLSConfig(logger, config.TLSConfig{
				CA:     realCAPath,
				Cert:   *certPath,
				Key:    *keyPath,
				SkipCA: skipCA,
			})
			if err != nil {
				return err
			}
		}
		ctx.Logger = logger.Named("cli")
		ctx.Client = &http.Client{
			Transport: &http.Transport{
				Proxy:           http.ProxyFromEnvironment,
				TLSClientConfig: tlsConfig,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:          100,
				IdleConnTimeout:       30 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		}
		ctx.CUrls = *curls
		ctx.SSL = tlsConfig != nil
		return nil
	}

	rootCmd.AddCommand(GetNamespaceCmd(ctx))
	rootCmd.AddCommand(GetConfigCmd(ctx))
	return rootCmd
}
