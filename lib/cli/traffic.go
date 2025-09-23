// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"net/http"
	"strconv"
	"syscall"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func GetTrafficCmd(ctx *Context) *cobra.Command {
	trafficCmd := &cobra.Command{
		Use:   "traffic [command]",
		Short: "",
	}
	trafficCmd.AddCommand(GetTrafficCaptureCmd(ctx))
	trafficCmd.AddCommand(GetTrafficReplayCmd(ctx))
	trafficCmd.AddCommand(GetTrafficCancelCmd(ctx))
	trafficCmd.AddCommand(GetTrafficShowCmd(ctx))
	return trafficCmd
}

func GetTrafficCaptureCmd(ctx *Context) *cobra.Command {
	captureCmd := &cobra.Command{
		Use:   "capture [flags]",
		Short: "",
	}
	output := captureCmd.PersistentFlags().String("output", "", "output directory for traffic files")
	duration := captureCmd.PersistentFlags().String("duration", "", "the duration of traffic capture")
	encrypt := captureCmd.PersistentFlags().String("encryption-method", "", "the encryption method used for encrypting traffic files")
	compress := captureCmd.PersistentFlags().Bool("compress", true, "whether compress the traffic files")
	captureCmd.RunE = func(cmd *cobra.Command, args []string) error {
		reader := GetFormReader(map[string]string{
			"output":         *output,
			"duration":       *duration,
			"encrypt-method": *encrypt,
			"compress":       strconv.FormatBool(*compress),
		})
		resp, err := doRequest(cmd.Context(), ctx, http.MethodPost, "/api/traffic/capture", reader)
		if err != nil {
			return err
		}

		cmd.Println(resp)
		return nil
	}
	return captureCmd
}

func GetTrafficReplayCmd(ctx *Context) *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   "replay [flags]",
		Short: "",
	}
	input := replayCmd.PersistentFlags().String("input", "", "directory for traffic files")
	speed := replayCmd.PersistentFlags().Float64("speed", 1, "replay speed")
	username := replayCmd.PersistentFlags().String("username", "", "the username to connect to TiDB for replay")
	password := replayCmd.PersistentFlags().String("password", "", "the password to connect to TiDB for replay")
	readonly := replayCmd.PersistentFlags().Bool("read-only", false, "only replay read-only queries, default is false")
	format := replayCmd.PersistentFlags().String("format", "", "the format of traffic files")
	cmdStartTime := replayCmd.PersistentFlags().String("command-start-time", "", "the start time to replay the traffic, format is RFC3339 or RFC3339Nano. The command before this start time will be ignored.")
	ignoreErrors := replayCmd.PersistentFlags().Bool("ignore-errs", false, "ignore errors when replaying")
	bufSize := replayCmd.PersistentFlags().Int("bufsize", 100000, "the size of buffer for reordering commands from audit files. 0 means no buffering.")
	psCloseStrategy := replayCmd.PersistentFlags().String("ps-close", "directed", "the strategy to close prepared statements. Supported values: directed (close when the original prepared statement closed), always (close the prepared statement right after it's executed), never (never close prepared statements). Default is directed.")
	replayCmd.RunE = func(cmd *cobra.Command, args []string) error {
		username := *username
		if len(username) == 0 {
			return errors.New("username is required")
		}
		password := *password
		if !cmd.Flags().Changed("password") {
			fmt.Printf("Input password for user %s: ", username)
			bytePassword, err := term.ReadPassword(int(syscall.Stdin))
			if err != nil {
				return err
			}
			password = string(bytePassword)
		}
		reader := GetFormReader(map[string]string{
			"input":        *input,
			"speed":        strconv.FormatFloat(*speed, 'f', -1, 64),
			"username":     username,
			"password":     password,
			"readonly":     strconv.FormatBool(*readonly),
			"format":       *format,
			"cmdstarttime": *cmdStartTime,
			"ignore-errs":  strconv.FormatBool(*ignoreErrors),
			"bufsize":      strconv.Itoa(*bufSize),
			"ps-close":     *psCloseStrategy,
		})
		resp, err := doRequest(cmd.Context(), ctx, http.MethodPost, "/api/traffic/replay", reader)
		if err != nil {
			return err
		}

		cmd.Println(resp)
		return nil
	}
	return replayCmd
}

func GetTrafficCancelCmd(ctx *Context) *cobra.Command {
	cancelCmd := &cobra.Command{
		Use:   "cancel",
		Short: "",
	}
	cancelCmd.RunE = func(cmd *cobra.Command, args []string) error {
		resp, err := doRequest(cmd.Context(), ctx, http.MethodPost, "/api/traffic/cancel", nil)
		if err != nil {
			return err
		}

		cmd.Println(resp)
		return nil
	}
	return cancelCmd
}

func GetTrafficShowCmd(ctx *Context) *cobra.Command {
	showCmd := &cobra.Command{
		Use:   "show",
		Short: "",
	}
	showCmd.RunE = func(cmd *cobra.Command, args []string) error {
		resp, err := doRequest(cmd.Context(), ctx, http.MethodGet, "/api/traffic/show", nil)
		if err != nil {
			return err
		}

		cmd.Println(resp)
		return nil
	}
	return showCmd
}
