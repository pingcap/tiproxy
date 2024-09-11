// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors_test

import (
	"testing"

	serr "github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestWrap(t *testing.T) {
	e1 := serr.New("tt")
	e2 := serr.New("dd")
	e := serr.Wrap(e1, e2)
	require.ErrorIsf(t, e, e1, "equal to the external error")
	require.ErrorAsf(t, e, &e2, "unwrapping to the internal error")
	require.Equal(t, e1, serr.Unwrap(e), "unwrapping to the internal error")

	require.Equal(t, e2, serr.Wrap(nil, e2), "wrap with nil got the original")
	require.Nil(t, serr.Wrap(e2, nil), "wrap nil got nil")
}

func TestWrapf(t *testing.T) {
	e1 := serr.New("tt")
	e2 := serr.New("dd")
	e := serr.Wrapf(e1, "%w: 4", e2)
	require.ErrorIsf(t, e, e1, "equal to the external error")
	require.ErrorIsf(t, e, e2, "equal to the underlying error, too")
	require.ErrorAsf(t, e, &e2, "unwrapping to the internal error")

	require.Nil(t, serr.Wrapf(nil, ""), "wrap nil got nil")
}
