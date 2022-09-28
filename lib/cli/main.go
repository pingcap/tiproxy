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
	"net/http"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func GetRootCmd(tlsConfig *tls.Config) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "tiproxyctl",
		Short:        "cli",
		SilenceUsage: true,
	}

	ctx := &Context{}

	curls := rootCmd.PersistentFlags().StringArray("curls", []string{"localhost:3080"}, "API gateway addresses")
	logEncoder := rootCmd.PersistentFlags().String("log_encoder", "tidb", "log in format of tidb, console, or json")
	logLevel := rootCmd.PersistentFlags().String("log_level", "info", "log level")
	insecure := rootCmd.PersistentFlags().BoolP("insecure", "k", false, "enable TLS without CA, useful for testing, or for expired certs")
	caPath := rootCmd.PersistentFlags().String("ca", "", "CA to verify server certificates, set to 'skip' if want to enable SSL without verification")
	certPath := rootCmd.PersistentFlags().String("cert", "", "cert for server-side client authentication")
	keyPath := rootCmd.PersistentFlags().String("key", "", "key for server-side client authentication")
	rootCmd.PersistentFlags().Bool("indent", true, "whether indent the returned json")
	rootCmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		zapcfg := zap.NewDevelopmentConfig()
		zapcfg.Encoding = *logEncoder
		if level, err := zap.ParseAtomicLevel(*logLevel); err == nil {
			zapcfg.Level = level
		}
		logger, err := zapcfg.Build()
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
