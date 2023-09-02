// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package versioninfo

import (
	_ "runtime" // import link package
	_ "unsafe"  // required by go:linkname
)

// These variables will be overwritten by Makefile.
var (
	TiProxyVersion   = "None"
	TiProxyGitBranch = "None"
	TiProxyGitHash   = "None"
	TiProxyBuildTS   = "None"
)

//go:linkname BuildVersion runtime.buildVersion
var BuildVersion string
