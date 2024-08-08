// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"os/exec"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

// ExecCmd executes commands with checking potential tainted input.
func ExecCmd(cmd string, args ...string) error {
	cmds := make([]string, 0, len(args)+1)
	if !isValidArg(cmd) {
		return errors.Errorf("invalid cmd: %s", cmd)
	}
	cmds = append(cmds, cmd)
	for _, arg := range args {
		if !isValidArg(arg) {
			return errors.Errorf("invalid argument: %s", arg)
		}
		cmds = append(cmds, arg)
	}
	output, err := exec.Command(cmds[0], cmds[1:]...).CombinedOutput()
	if err != nil {
		return errors.Wrapf(errors.WithStack(err), "output: %s", string(output))
	}
	return nil
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
