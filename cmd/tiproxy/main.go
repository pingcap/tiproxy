// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/cmd"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/pingcap/tiproxy/pkg/server"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
	"github.com/spf13/cobra"
)

func rewriteSingleDashLongFlags(args []string) []string {
	// Cobra/pflag uses:
	// - short flags:  -v
	// - long flags:   --config=/path or --config /path
	//
	// Some users may accidentally pass "-config" / "-query-interaction-metrics" (single dash).
	// pflag treats that as a shorthand bundle and errors. We rewrite a small allow-list to be lenient.
	allow := map[string]string{
		"config":                    "config",
		"log_encoder":               "log_encoder",
		"log_level":                 "log_level",
		"query-interaction-metrics": "query-interaction-metrics",
		"query_interaction_metrics": "query-interaction-metrics",
	}
	out := make([]string, 0, len(args))
	for _, a := range args {
		if len(a) < 3 || !strings.HasPrefix(a, "-") || strings.HasPrefix(a, "--") {
			out = append(out, a)
			continue
		}
		name := a[1:]
		suffix := ""
		if i := strings.IndexByte(name, '='); i >= 0 {
			suffix = name[i:]
			name = name[:i]
		}
		if canonical, ok := allow[name]; ok {
			out = append(out, "--"+canonical+suffix)
			continue
		}
		out = append(out, a)
	}
	return out
}

func main() {
	rootCmd := &cobra.Command{
		Use:     os.Args[0],
		Short:   "start the proxy server",
		Version: fmt.Sprintf("%s, commit %s", versioninfo.TiProxyVersion, versioninfo.TiProxyGitHash),
	}
	rootCmd.SetOutput(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	sctx := &sctx.Context{}

	rootCmd.PersistentFlags().StringVar(&sctx.ConfigFile, "config", "", "proxy config file path")
	rootCmd.PersistentFlags().StringVar(&sctx.Overlay.Log.Encoder, "log_encoder", "", "log in format of tidb, console, or json")
	rootCmd.PersistentFlags().StringVar(&sctx.Overlay.Log.Level, "log_level", "", "log level")
	rootCmd.PersistentFlags().BoolVar(&sctx.Overlay.Advance.QueryInteractionMetrics, "query-interaction-metrics", false, "enable query interaction latency metrics (advance.query-interaction-metrics). Note: CLI flag is an overlay and will override config reloads")
	// Keep underscore alias for compatibility with existing flag naming style.
	rootCmd.PersistentFlags().BoolVar(&sctx.Overlay.Advance.QueryInteractionMetrics, "query_interaction_metrics", false, "alias of --query-interaction-metrics")

	rootCmd.SetArgs(rewriteSingleDashLongFlags(os.Args[1:]))

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
