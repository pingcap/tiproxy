// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/cmd"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/manager/logger"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/server/api"
	replaycmd "github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	mgrrp "github.com/pingcap/tiproxy/pkg/sqlreplay/manager"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     os.Args[0],
		Short:   "start the replayer",
		Version: fmt.Sprintf("%s, commit %s", versioninfo.TiProxyVersion, versioninfo.TiProxyGitHash),
	}
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	addr := rootCmd.PersistentFlags().String("addr", "127.0.0.1:4000", "the downstream address to connect to")
	input := rootCmd.PersistentFlags().String("input", "", "directory for traffic files")
	speed := rootCmd.PersistentFlags().Float64("speed", 1, "replay speed")
	username := rootCmd.PersistentFlags().String("username", "root", "the username to connect to TiDB for replay")
	password := rootCmd.PersistentFlags().String("password", "", "the password to connect to TiDB for replay")
	readonly := rootCmd.PersistentFlags().Bool("read-only", false, "only replay read-only queries, default is false")
	format := rootCmd.PersistentFlags().String("format", "", "the format of traffic files")
	logFile := rootCmd.PersistentFlags().String("log-file", "", "the output log file")
	cmdStartTime := rootCmd.PersistentFlags().Time("command-start-time", time.Time{}, []string{time.RFC3339, time.RFC3339Nano}, "the start time to replay the traffic, format is RFC3339. The command before this start time will be ignored.")
	cmdEndTime := rootCmd.PersistentFlags().Time("command-end-time", time.Time{}, []string{time.RFC3339, time.RFC3339Nano}, "the end time to replay the traffic, format is RFC3339. The command whose end ts is before this end time will be ignored.")
	ignoreErrs := rootCmd.PersistentFlags().Bool("ignore-errs", false, "ignore errors when replaying")
	bufSize := rootCmd.PersistentFlags().Int("bufsize", 100000, "the size of buffer for reordering commands from audit files. 0 means no buffering.")
	pprofAddr := rootCmd.PersistentFlags().String("pprof-addr", "", "the address to listen on for pprof, e.g. localhost:6060. By default pprof is disabled.")
	psCloseStrategy := rootCmd.PersistentFlags().String("ps-close", "directed", "the strategy to close prepared statements. Supported values: directed (close when the original prepared statement closed), always (close the prepared statement right after it's executed), never (never close prepared statements). Default is directed.")
	dryRun := rootCmd.PersistentFlags().Bool("dry-run", false, "dry run, don't connect to TiDB")
	checkPointFilePath := rootCmd.PersistentFlags().String("checkpoint-path", "", "the file path to store replay checkpoint information. If the file exists and not empty, the internal state will be loaded from the file to resume replaying.")
	dynamicInput := rootCmd.PersistentFlags().Bool("dynamic-input", false, "enable dynamic input mode, which watches the input directory for new traffic folders and replays them automatically.")
	replayerCount := rootCmd.PersistentFlags().Int("replayer-count", 1, "the total number of replayer instances running concurrently. Used only when dynamic-input is enabled.")
	replayerIndex := rootCmd.PersistentFlags().Int("replayer-index", 0, "the index of this replayer instance. Used only when dynamic-input is enabled.")

	rootCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		// set up general managers
		cfg := &config.Config{
			Log: config.Log{
				LogOnline: config.LogOnline{
					Level:   "info",
					LogFile: config.LogFile{Filename: *logFile},
				},
			},
			API: config.API{
				Addr: *pprofAddr,
			},
			EnableTrafficReplay: true,
		}
		lgMgr, lg, err := logger.NewLoggerManager(&cfg.Log)
		if err != nil {
			return err
		}

		// create replay job manager
		hsHandler := newStaticHandshakeHandler(*addr)
		idMgr := id.NewIDManager()
		r := mgrrp.NewJobManager(lg, cfg, &nopCertManager{}, idMgr, hsHandler)

		// start api server
		mgrs := api.Managers{
			CfgMgr:        &nopConfigManager{cfg: cfg},
			NsMgr:         nil,
			CertMgr:       cert.NewCertManager(),
			BackendReader: nil,
			ReplayJobMgr:  r,
		}
		var ready atomic.Bool
		ready.Store(true)
		apiServer, err := api.NewServer(cfg.API, lg.Named("api"), mgrs, nil, &ready)
		if err != nil {
			return err
		}

		// set up signal handler
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			sc := make(chan os.Signal, 1)
			signal.Notify(sc,
				syscall.SIGINT,
				syscall.SIGTERM,
				syscall.SIGQUIT,
			)

			select {
			case <-sc:
				r.Stop(mgrrp.CancelConfig{Type: mgrrp.Replay})
			case <-ctx.Done():
			}
		}()

		// start replay
		replayCfg := replay.ReplayConfig{
			Input:              *input,
			Speed:              *speed,
			Username:           *username,
			Password:           *password,
			Format:             *format,
			ReadOnly:           *readonly,
			StartTime:          time.Now(),
			CommandStartTime:   *cmdStartTime,
			CommandEndTime:     *cmdEndTime,
			IgnoreErrs:         *ignoreErrs,
			BufSize:            *bufSize,
			PSCloseStrategy:    replaycmd.PSCloseStrategy(*psCloseStrategy),
			DryRun:             *dryRun,
			CheckPointFilePath: *checkPointFilePath,
			DynamicInput:       *dynamicInput,
			ReplayerCount:      *replayerCount,
			ReplayerIndex:      *replayerIndex,
		}
		if err := r.StartReplay(replayCfg); err != nil {
			cancel()
			return err
		}
		r.Wait()

		cancel()
		r.Close()
		_ = apiServer.Close()
		_ = lgMgr.Close()
		return nil
	}

	cmd.RunRootCommand(rootCmd)
}

var _ mgrrp.CertManager = &nopCertManager{}

type nopCertManager struct{}

func (c *nopCertManager) SQLTLS() *tls.Config {
	return nil
}

type staticHandshakeHandler struct {
	rt router.Router
}

func newStaticHandshakeHandler(addr string) *staticHandshakeHandler {
	return &staticHandshakeHandler{
		rt: router.NewStaticRouter([]string{addr}),
	}
}

func (handler *staticHandshakeHandler) HandleHandshakeResp(backend.ConnContext, *pnet.HandshakeResp) error {
	return nil
}

func (handler *staticHandshakeHandler) HandleHandshakeErr(backend.ConnContext, *mysql.MyError) bool {
	return false
}

func (handler *staticHandshakeHandler) GetRouter(backend.ConnContext, *pnet.HandshakeResp) (router.Router, error) {
	return handler.rt, nil
}

func (handler *staticHandshakeHandler) OnHandshake(backend.ConnContext, string, error, backend.ErrorSource) {
}

func (handler *staticHandshakeHandler) OnTraffic(backend.ConnContext) {
}

func (handler *staticHandshakeHandler) OnConnClose(backend.ConnContext, backend.ErrorSource) error {
	return nil
}

func (handler *staticHandshakeHandler) GetCapability() pnet.Capability {
	return backend.SupportedServerCapabilities
}

func (handler *staticHandshakeHandler) GetServerVersion() string {
	return pnet.ServerVersion
}

var _ api.ConfigManager = (*nopConfigManager)(nil)

type nopConfigManager struct {
	api.ConfigManager
	cfg *config.Config
}

func (n *nopConfigManager) GetConfig() *config.Config {
	return n.cfg
}
