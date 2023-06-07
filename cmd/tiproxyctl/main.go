// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/TiProxy/lib/cli"
	"github.com/pingcap/TiProxy/lib/util/cmd"
)

var (
	Version = "test"
	Commit  = "test commit"
)

func main() {
	rootCmd := cli.GetRootCmd(nil)
	rootCmd.Version = fmt.Sprintf("%s, commit %s", Version, Commit)
	rootCmd.Use = strings.Replace(rootCmd.Use, "tiproxyctl", os.Args[0], 1)
	cmd.RunRootCommand(rootCmd)
}
