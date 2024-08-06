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
		hasErr bool
	}{
		{
			cmds:   []string{"echo", "$(whoami)"},
			hasErr: true,
		},
		{
			cmds: []string{"echo", "abc"},
		},
		{
			cmds:   []string{"hello"},
			hasErr: true,
		},
	}

	for i, test := range tests {
		err := ExecCmd(test.cmds[0], test.cmds[1:]...)
		require.Equal(t, test.hasErr, err != nil, "case %d", i)
	}
}
