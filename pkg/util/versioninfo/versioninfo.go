// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package versioninfo

import (
	"fmt"

	semver "github.com/Masterminds/semver"
)

// These variables will be overwritten by Makefile.
var (
	TiProxyVersion   = "None"
	TiProxyGitBranch = "None"
	TiProxyGitHash   = "None"
	TiProxyBuildTS   = "None"
)

// Returns whether v1 >= v2.
// If any error happens, return true.
func GtEqToVersion(v1, v2 string) bool {
	constraint, err := semver.NewConstraint(fmt.Sprintf(">=%s", v2))
	if err != nil {
		return true
	}
	ver, err := semver.NewVersion(v1)
	if err != nil {
		return true
	}
	// v8.5.0-alpha is treated as v8.5.0
	ckVer := *ver
	if ver.Prerelease() != "" {
		ckVer, err = ver.SetPrerelease("")
		if err != nil {
			return true
		}
	}
	return constraint.Check(&ckVer)
}
