// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/pingcap/TiProxy/lib/util/cmd"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/sctx"
	"github.com/pingcap/TiProxy/pkg/server"
	"github.com/spf13/cobra"
)

var (
	Version = "test"
	Commit  = "test commit"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     os.Args[0],
		Short:   "start the proxy server",
		Version: fmt.Sprintf("%s, commit %s", Version, Commit),
	}
	rootCmd.SetOutput(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	sctx := &sctx.Context{}

	rootCmd.PersistentFlags().StringVar(&sctx.ConfigFile, "config", "", "proxy config file path")
	rootCmd.PersistentFlags().StringVar(&sctx.Overlay.Log.Encoder, "log_encoder", "", "log in format of tidb, console, or json")
	rootCmd.PersistentFlags().StringVar(&sctx.Overlay.Log.Level, "log_level", "", "log level")

	metrics.MaxProcsGauge.Set(float64(runtime.GOMAXPROCS(0)))

	rootCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		srv, err := server.NewServer(cmd.Context(), sctx)
		if err != nil {
			return errors.Wrapf(err, "fail to create server")
		}

		<-cmd.Context().Done()
		if e := srv.Close(); e != nil {
			err = errors.Wrapf(err, "shutdown with errors")
		}

		return err
	}

	cmd.RunRootCommand(rootCmd)
}
