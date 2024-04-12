// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/pingcap/tiproxy/lib/util/cmd"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/pingcap/tiproxy/pkg/server"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     os.Args[0],
		Short:   "start the proxy server",
		Version: fmt.Sprintf("%s, commit %s", versioninfo.TiProxyVersion, versioninfo.TiProxyGitHash),
	}
	rootCmd.SetOutput(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	sctx := &sctx.Context{}

	var deprecatedStr string
	rootCmd.PersistentFlags().StringVar(&sctx.ConfigFile, "config", "", "proxy config file path")
	rootCmd.PersistentFlags().StringVar(&deprecatedStr, "log_encoder", "", "deprecated and will be removed")
	rootCmd.PersistentFlags().StringVar(&deprecatedStr, "log_level", "", "deprecated and will be removed")
	rootCmd.PersistentFlags().StringVar(&sctx.Overlay.Proxy.AdvertiseAddr, "advertise-addr", "", "advertise address")

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
