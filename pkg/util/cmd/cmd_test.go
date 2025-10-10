// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecCmd(t *testing.T) {
	tests := []struct {
		cmds   []string
		output string
		hasErr bool
	}{
		{
			cmds:   []string{"echo", "$(whoami)"},
			hasErr: true,
		},
		{
			cmds:   []string{"echo", "abc"},
			output: "abc\n",
		},
		{
			cmds: []string{"cd", "."},
		},
		{
			cmds:   []string{"hello"},
			hasErr: true,
		},
	}

	for i, test := range tests {
		output, err := ExecCmd(test.cmds[0], test.cmds[1:]...)
		require.Equal(t, test.hasErr, err != nil, "case %d", i)
		require.Equal(t, test.output, output)
	}
}
