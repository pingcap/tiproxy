// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"os/exec"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/siddontang/go/hack"
)

// ExecCmd executes commands with checking potential tainted input.
func ExecCmd(ctx context.Context, cmd string, args ...string) (string, error) {
	if !isValidArg(cmd) {
		return "", errors.Errorf("invalid cmd: %s", cmd)
	}
	for _, arg := range args {
		if !isValidArg(arg) {
			return "", errors.Errorf("invalid argument: %s", arg)
		}
	}
	output, err := exec.CommandContext(ctx, cmd, args...).CombinedOutput()
	if err != nil {
		return hack.String(output), errors.Wrapf(errors.WithStack(err), "output: %s", string(output))
	}
	return hack.String(output), nil
}

func isValidArg(arg string) bool {
	dangerousChars := []string{";", "&", "|", "`", "$(", "${", "<", ">", ">>"}
	for _, char := range dangerousChars {
		if strings.Contains(arg, char) {
			return false
		}
	}
	return true
}
