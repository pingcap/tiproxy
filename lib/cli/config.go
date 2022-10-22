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
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

const (
	configPrefix = "/api/admin/config"
)

func getConfigCmd(ctx *Context, pathSuffix string) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: pathSuffix,
	}
	path := fmt.Sprintf("%s/%s", configPrefix, pathSuffix)

	// set config proxy
	{
		setProxy := &cobra.Command{
			Use: "set",
		}
		input := setProxy.Flags().String("input", "", "specify the input json file for proxy config")
		setProxy.RunE = func(cmd *cobra.Command, args []string) error {
			b := cmd.InOrStdin()
			if *input != "" {
				f, err := os.Open(*input)
				if err != nil {
					return err
				}
				defer f.Close()
				b = f
			}

			resp, err := doRequest(cmd.Context(), ctx, http.MethodPut, path, b)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(setProxy)
	}

	// get config proxy
	{
		getProxy := &cobra.Command{
			Use: "get",
		}
		getProxy.RunE = func(cmd *cobra.Command, args []string) error {
			resp, err := doRequest(cmd.Context(), ctx, http.MethodGet, path, nil)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(getProxy)
	}

	return rootCmd
}

func GetConfigCmd(ctx *Context) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "config",
		Short: "",
	}
	rootCmd.AddCommand(getConfigCmd(ctx, "proxy"))
	rootCmd.AddCommand(getConfigCmd(ctx, "log"))
	return rootCmd
}
