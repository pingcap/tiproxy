// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

const (
	configPrefix = "/api/admin/config/"
)

func GetConfigCmd(ctx *Context) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "config",
		Short: "",
	}

	// set config
	{
		setProxy := &cobra.Command{
			Use: "set",
		}
		input := setProxy.Flags().String("input", "", "specify the input toml file for proxy config")
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

			resp, err := doRequest(cmd.Context(), ctx, http.MethodPut, configPrefix, b)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(setProxy)
	}

	// get config
	{
		getProxy := &cobra.Command{
			Use: "get",
		}
		getProxy.RunE = func(cmd *cobra.Command, args []string) error {
			resp, err := doRequest(cmd.Context(), ctx, http.MethodGet, configPrefix, nil)
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
