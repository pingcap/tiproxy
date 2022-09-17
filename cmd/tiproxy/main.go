// Copyright 2020 Ipalfish, Inc.
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

package main

import (
	"io/ioutil"
	"os"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/cmd"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/server"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   os.Args[0],
		Short: "start the proxy server",
	}

	configFile := rootCmd.PersistentFlags().String("config", "conf/proxy.yaml", "proxy config file path")
	pubAddr := rootCmd.PersistentFlags().String("pub_addr", "127.0.0.1", "IP or domain, will be used as the accessible addr for other clients")
	logEncoder := rootCmd.PersistentFlags().String("log_encoder", "", "log in format of tidb, console, or json")
	logLevel := rootCmd.PersistentFlags().String("log_level", "", "log level")

	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		proxyConfigData, err := ioutil.ReadFile(*configFile)
		if err != nil {
			return err
		}

		cfg, err := config.NewConfig(proxyConfigData)
		if err != nil {
			return err
		}

		if *logEncoder != "" {
			cfg.Log.Encoder = *logEncoder
		}
		if *logLevel != "" {
			cfg.Log.Level = *logLevel
		}

		zapcfg := zap.NewDevelopmentConfig()
		zapcfg.Encoding = cfg.Log.Encoder
		zapcfg.DisableStacktrace = true
		if level, err := zap.ParseAtomicLevel(cfg.Log.Level); err == nil {
			zapcfg.Level = level
		}
		logger, err := zapcfg.Build()
		if err != nil {
			return err
		}
		logger = logger.Named("main")

		srv, err := server.NewServer(cmd.Context(), cfg, logger, *pubAddr)
		if err != nil {
			return errors.Wrapf(err, "fail to create server")
		}

		var wg waitgroup.WaitGroup
		wg.Run(func() { srv.Run(cmd.Context()) })

		<-cmd.Context().Done()
		if e := srv.Close(); e != nil {
			err = errors.Wrapf(err, "shutdown with errors")
		} else {
			logger.Info("gracefully shutdown")
		}

		wg.Wait()
		return err
	}

	cmd.RunRootCommand(rootCmd)
}
