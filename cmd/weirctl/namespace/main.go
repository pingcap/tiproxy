package namespace

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/djshow832/weir/cmd/weirctl/util"
	"github.com/spf13/cobra"
)

const (
	prefix = "/api/admin/namespace"
)

func GetRootCommand(ctx *util.Context) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "namespace",
		Short: "",
	}

	// list all namespaces
	rootCmd.AddCommand(
		&cobra.Command{
			Use: "list",
			RunE: func(cmd *cobra.Command, args []string) error {
				resp, err := util.DoRequest(cmd.Context(), ctx, http.MethodGet, prefix, nil)
				if err != nil {
					return err
				}

				fmt.Print(resp)
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
			resp, err := util.DoRequest(cmd.Context(), ctx, http.MethodPost, fmt.Sprintf("%s/commit?namespaces=%s", prefix, strings.Join(args, ",")), nil)
			if err != nil {
				return err
			}

			fmt.Print(resp)
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

			resp, err := util.DoRequest(cmd.Context(), ctx, http.MethodGet, fmt.Sprintf("%s/%s", prefix, args[0]), nil)
			if err != nil {
				return err
			}

			fmt.Print(resp)
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

			resp, err := util.DoRequest(cmd.Context(), ctx, http.MethodPut, fmt.Sprintf("%s/%s", prefix, args[0]), in)
			if err != nil {
				return err
			}

			fmt.Print(resp)
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

			resp, err := util.DoRequest(cmd.Context(), ctx, http.MethodDelete, fmt.Sprintf("%s/%s", prefix, args[0]), nil)
			if err != nil {
				return err
			}

			fmt.Print(resp)
			return nil
		}
		rootCmd.AddCommand(delNamespace)
	}

	return rootCmd
}
