// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"net/http"

	"github.com/spf13/cobra"
)

const (
	healthPrefix = "/api/debug/health"
)

func GetHealthCmd(ctx *Context) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "health",
		Short: "",
	}

	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		resp, err := doRequest(cmd.Context(), ctx, http.MethodGet, healthPrefix, nil)
		if err != nil {
			return err
		}

		cmd.Println(resp)
		return nil
	}

	return rootCmd
}
