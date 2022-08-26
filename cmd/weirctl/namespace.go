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
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const (
	namespacePrefix = "/api/admin/namespace"
)

func GetNamespaceCmd(ctx *Context) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "namespace",
		Short: "",
	}

	// list all namespaces
	rootCmd.AddCommand(
		&cobra.Command{
			Use: "list",
			RunE: func(cmd *cobra.Command, args []string) error {
				resp, err := doRequest(cmd.Context(), ctx, http.MethodGet, namespacePrefix, nil)
				if err != nil {
					return err
				}

				cmd.Println(resp)
				return nil
			},
		},
	)

	// refresh namespaces
	{
		commitNamespaces := &cobra.Command{
			Use: "commit",
		}
		commitNamespaces.RunE = func(cmd *cobra.Command, args []string) error {
			resp, err := doRequest(cmd.Context(), ctx, http.MethodPost, fmt.Sprintf("%s/commit?namespaces=%s", namespacePrefix, strings.Join(args, ",")), nil)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(commitNamespaces)
	}

	// get specific namespace
	{
		getNamespace := &cobra.Command{
			Use: "get",
		}
		getNamespace.RunE = func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}

			resp, err := doRequest(cmd.Context(), ctx, http.MethodGet, fmt.Sprintf("%s/%s", namespacePrefix, args[0]), nil)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(getNamespace)
	}

	// put specific namespace
	{
		putNamespace := &cobra.Command{
			Use: "put",
		}
		ns := putNamespace.Flags().String("ns", "-", "file")
		putNamespace.RunE = func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}

			in := cmd.InOrStdin()
			if *ns != "-" {
				f, err := os.Open(*ns)
				if err != nil {
					return err
				}
				defer f.Close()
				in = f
			}

			resp, err := doRequest(cmd.Context(), ctx, http.MethodPut, fmt.Sprintf("%s/%s", namespacePrefix, args[0]), in)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(putNamespace)
	}

	// delete specific namespace
	{
		delNamespace := &cobra.Command{
			Use: "del",
		}
		delNamespace.RunE = func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}

			resp, err := doRequest(cmd.Context(), ctx, http.MethodDelete, fmt.Sprintf("%s/%s", namespacePrefix, args[0]), nil)
			if err != nil {
				return err
			}

			cmd.Println(resp)
			return nil
		}
		rootCmd.AddCommand(delNamespace)
	}

	return rootCmd
}
