// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestFailException(t *testing.T) {
	commands := []cmd.Command{
		{ConnID: 1, Type: pnet.ComFieldList},
		{ConnID: 1, Type: pnet.ComQuery, Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)},
	}
	for i, command := range commands {
		exception := NewFailException(errors.New("mock error"), &command)
		require.Equal(t, Fail, exception.Type(), "case %d", i)
		require.Equal(t, uint64(1), exception.ConnID(), "case %d", i)
		require.Equal(t, "mock error", exception.Error(), "case %d", i)
	}
}

func TestOtherException(t *testing.T) {
	errs := []error{
		errors.New("mock error"),
		errors.Wrapf(errors.New("mock error"), "wrap"),
		errors.WithStack(errors.New("mock error")),
		errors.WithStack(errors.Wrapf(errors.New("mock error"), "wrap")),
	}
	for i, err := range errs {
		exception := NewOtherException(err, 1)
		require.Equal(t, Other, exception.Type(), "case %d", i)
		require.Equal(t, uint64(1), exception.ConnID(), "case %d", i)
		require.Equal(t, "mock error", exception.Key(), "case %d", i)
	}
}
