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
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

const (
	configPrefix = "/api/admin/config"
)

func GetConfigCmd(ctx *Context) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "config",
		Short: "",
	}

	// config proxy
	{
		configProxy := &cobra.Command{
			Use: "proxy",
		}
		input := configProxy.Flags().String("input", "", "specify the input json file for proxy config")
		configProxy.RunE = func(cmd *cobra.Command, args []string) error {
			var b io.Reader
			if *input != "" {
				f, err := os.Open(*input)
				if err != nil {
					return err
				}
				defer f.Close()
			} else {
				b = os.Stdin
			}

			resp, err := doRequest(cmd.Context(), ctx, http.MethodPut, fmt.Sprintf("%s/proxy", configPrefix), b)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(configProxy)
	}

	return rootCmd
}
