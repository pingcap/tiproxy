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

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var registerEncoders sync.Once

func RunRootCommand(rootCmd *cobra.Command) {
	registerEncoders.Do(func() {
		zap.RegisterEncoder("tidb", func(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
			return NewTiDBEncoder(cfg), nil
		})
	})

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
		fmt.Println(err)
		os.Exit(1)
	}
}
