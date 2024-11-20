// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
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
	encrypt := captureCmd.PersistentFlags().String("encrypt-method", "", "the encryption method used for encrypting traffic files")
	captureCmd.RunE = func(cmd *cobra.Command, args []string) error {
		reader := GetFormReader(map[string]string{"output": *output, "duration": *duration, "encrypt-method": *encrypt})
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
	readonly := replayCmd.PersistentFlags().Bool("readonly", false, "only replay read-only queries, default is false")
	replayCmd.RunE = func(cmd *cobra.Command, args []string) error {
		reader := GetFormReader(map[string]string{
			"input":    *input,
			"speed":    strconv.FormatFloat(*speed, 'f', -1, 64),
			"username": *username,
			"password": *password,
			"readonly": strconv.FormatBool(*readonly),
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
