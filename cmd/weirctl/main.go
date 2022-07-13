package main

import (
	"net/http"
	"time"

	"github.com/djshow832/weir/cmd/weirctl/namespace"
	"github.com/djshow832/weir/cmd/weirctl/util"
	"github.com/djshow832/weir/pkg/util/cmd"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "weirctl",
		Short: "cli",
	}

	ctx := &util.Context{}

	curls := rootCmd.PersistentFlags().StringArray("curls", []string{"localhost:2379"}, "API gateway addresses")
	logEncoder := rootCmd.PersistentFlags().String("log_encoder", "tidb", "log in format of tidb, console, or json")
	logLevel := rootCmd.PersistentFlags().String("log_level", "info", "log level")
	rootCmd.PersistentFlags().Bool("indent", true, "whether indent the returned json")
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		zapcfg := zap.NewDevelopmentConfig()
		zapcfg.EncoderConfig.EncodeTime = func(t time.Time, pae zapcore.PrimitiveArrayEncoder) {
			s := t.Format("2006/01/02 15:04:05.000 -07:00")
			pae.AppendString(s)
		}
		zapcfg.Encoding = *logEncoder
		if level, err := zap.ParseAtomicLevel(*logLevel); err == nil {
			zapcfg.Level = level
		}
		logger, err := zapcfg.Build()
		if err != nil {
			return err
		}
		ctx.Logger = logger.Named("cli")
		ctx.Client = &http.Client{}
		ctx.CUrls = *curls
		return nil
	}

	rootCmd.AddCommand(namespace.GetRootCommand(ctx))
	cmd.RunRootCommand(rootCmd)
}
