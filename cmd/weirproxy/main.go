// Copyright 2020 Ipalfish, Inc.
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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/proxy"
	"github.com/djshow832/weir/pkg/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	configFilePath = flag.String("config", "conf/weirproxy.yaml", "weir proxy config file path")
)

func main() {
	flag.Parse()
	proxyConfigData, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		fmt.Printf("read config file error: %v\n", err)
		os.Exit(1)
	}

	proxyCfg, err := config.UnmarshalProxyConfig(proxyConfigData)
	if err != nil {
		fmt.Printf("parse config file error: %v\n", err)
		os.Exit(1)
	}

	err = disk.InitializeTempDir(proxyCfg.ProxyServer.StoragePath)
	if err != nil {
		fmt.Printf("initialize temporary path error: %v\n", err)
		os.Exit(1)
	}

	p := proxy.NewProxy(proxyCfg)

	if err = p.Init(); err != nil {
		fmt.Printf("proxy init error: %v\n", err)
		p.Close()
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGPIPE,
		syscall.SIGUSR1,
	)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			sig := <-sc
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				logutil.BgLogger().Warn("get os signal, close proxy server", zap.String("signal", sig.String()))
				p.Close()
				break
			} else {
				logutil.BgLogger().Warn("ignore os signal", zap.String("signal", sig.String()))
			}
		}
	}()

	if err := p.Run(); err != nil {
		logutil.BgLogger().Error("proxy run error, exit", zap.Error(err))
	}

	wg.Wait()
}
