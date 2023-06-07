// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func RunRootCommand(rootCmd *cobra.Command) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sc := make(chan os.Signal, 1)
		signal.Notify(sc,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
		)

		// wait for quit signals
		<-sc
		cancel()
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
}
