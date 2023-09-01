// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors_test

import (
	gerr "errors"
	"fmt"
	"testing"

	serr "github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestStacktrace(t *testing.T) {
	e := serr.WithStack(serr.New("tt"))
	require.Equal(t, fmt.Sprintf("%s", e), "tt")
	require.Contains(t, fmt.Sprintf("%+v", e), t.Name(), "stacktrace must contain test name")
	require.Contains(t, fmt.Sprintf("%v", e), t.Name(), "stacktrace must contain test name")
	require.Contains(t, fmt.Sprintf("%+s", e), t.Name(), "stacktrace must contain test name")

	require.Nil(t, serr.WithStack(nil), "wrap nil got nil")
}

func TestUnwrap(t *testing.T) {
	e1 := gerr.New("t")
	e2 := serr.WithStack(e1)
	e3 := serr.WithStack(e2)
	require.Equal(t, nil, gerr.Unwrap(e2), "unwrapped error will skip stackrace")
	require.Equal(t, nil, gerr.Unwrap(e3), "unwrapped error will skip stackrace")
	require.ErrorIs(t, e2, e1, "stacktrace does not affect Is")
	require.ErrorAs(t, e2, &e1, "stacktrace does not affect As")
}

func TestWarning(t *testing.T) {
	warning := &serr.Warning{Err: serr.Wrapf(serr.New("internal err"), "msg")}
	var w *serr.Warning
	require.True(t, serr.As(warning, &w))
	require.Equal(t, "internal err: msg", warning.Error())
}
