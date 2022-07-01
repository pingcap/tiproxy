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
