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
	"fmt"
	"os"

	"github.com/pingcap/TiProxy/lib/util/cmd"
	"github.com/pingcap/TiProxy/lib/util/errors"
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
