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
	"os"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/cmd"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/sctx"
	"github.com/pingcap/TiProxy/pkg/server"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   os.Args[0],
		Short: "start the proxy server",
	}

	configFile := rootCmd.PersistentFlags().String("config", "conf/proxy.yaml", "proxy config file path")
	logEncoder := rootCmd.PersistentFlags().String("log_encoder", "", "log in format of tidb, console, or json")
	logLevel := rootCmd.PersistentFlags().String("log_level", "", "log level")
	clusterName := rootCmd.PersistentFlags().String("cluster_name", "tiproxy", "default cluster name, used to generate node name and differential clusters in dns discovery")
	nodeName := rootCmd.PersistentFlags().String("node_name", "", "by default, it is generate prefixed by cluster-name")
	pubAddr := rootCmd.PersistentFlags().String("pub_addr", "127.0.0.1", "IP or domain, will be used as the accessible addr for others")
	bootstrapClusters := rootCmd.PersistentFlags().StringSlice("bootstrap_clusters", []string{}, "lists of other nodes in the cluster, e.g. 'n1=xxx,n2=xxx', where xx are IPs or domains")
	bootstrapDiscoveryUrl := rootCmd.PersistentFlags().String("bootstrap_discovery_etcd", "", "etcd discovery service url")
	bootstrapDiscoveryDNS := rootCmd.PersistentFlags().String("bootstrap_discovery_dns", "", "dns srv discovery")

	rootCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		proxyConfigData, err := os.ReadFile(*configFile)
		if err != nil {
			return err
		}

		cfg, err := config.NewConfig(proxyConfigData)
		if err != nil {
			return err
		}

		if *logEncoder != "" {
			cfg.Log.Encoder = *logEncoder
		}
		if *logLevel != "" {
			cfg.Log.Level = *logLevel
		}

		sctx := &sctx.Context{
			Config: cfg,
			Cluster: sctx.Cluster{
				PubAddr:           *pubAddr,
				ClusterName:       *clusterName,
				NodeName:          *nodeName,
				BootstrapDurl:     *bootstrapDiscoveryUrl,
				BootstrapDdns:     *bootstrapDiscoveryDNS,
				BootstrapClusters: *bootstrapClusters,
			},
		}

		srv, err := server.NewServer(cmd.Context(), sctx)
		if err != nil {
			return errors.Wrapf(err, "fail to create server")
		}

		var wg waitgroup.WaitGroup
		wg.Run(func() { srv.Run(cmd.Context()) })

		<-cmd.Context().Done()
		if e := srv.Close(); e != nil {
			err = errors.Wrapf(err, "shutdown with errors")
		}

		wg.Wait()
		return err
	}

	cmd.RunRootCommand(rootCmd)
}
