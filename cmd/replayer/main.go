// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/cmd"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/manager/logger"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	mgrrp "github.com/pingcap/tiproxy/pkg/sqlreplay/manager"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
	"github.com/spf13/cobra"
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

	rootCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		replayCfg := replay.ReplayConfig{
			Input:     *input,
			Speed:     *speed,
			Username:  *username,
			Password:  *password,
			Format:    *format,
			ReadOnly:  *readonly,
			StartTime: time.Now(),
		}

		r := &replayer{}
		if err := r.initComponents(*addr, *logFile); err != nil {
			return err
		}
		if err := r.start(replayCfg); err != nil {
			return err
		}
		return nil
	}

	cmd.RunRootCommand(rootCmd)
}

type replayer struct {
	lgMgr  *logger.LoggerManager
	replay mgrrp.JobManager
	idMgr  *id.IDManager
}

func (r *replayer) initComponents(addr, logFile string) error {
	lgMgr, lg, err := logger.NewLoggerManager(&config.Log{
		LogOnline: config.LogOnline{
			Level:   "info",
			LogFile: config.LogFile{Filename: logFile},
		},
	})
	if err != nil {
		return err
	}
	r.lgMgr = lgMgr
	hsHandler := newStaticHandshakeHandler(addr)
	idMgr := id.NewIDManager()
	cfg := config.NewConfig()
	r.replay = mgrrp.NewJobManager(lg.Named("replay"), cfg, &nopCertManager{}, idMgr, hsHandler)
	return nil
}

func (r *replayer) start(replayCfg replay.ReplayConfig) error {
	if err := r.replay.StartReplay(replayCfg); err != nil {
		return err
	}
	r.replay.Wait()
	return nil
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
