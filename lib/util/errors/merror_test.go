// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors_test

import (
	"testing"

	serr "github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestCollect(t *testing.T) {
	e1 := serr.New("tt")
	e2 := serr.New("dd")
	e3 := serr.New("dd")
	e := serr.Collect(e1, e2, e3)

	require.ErrorIsf(t, e, e1, "equal to the external error")
	require.Equal(t, nil, serr.Unwrap(e), "unwrapping stops here")
	require.ErrorIsf(t, e, e1, "but errors.Is works for all errors")
	require.Equal(t, e.(*serr.MError).Cause(), []error{e2, e3}, "get underlying errors")
	require.NoError(t, serr.Collect(e3), "nil if there is no underlying error")

	e4 := serr.Collect(e1, e2, nil).(*serr.MError)
	require.Len(t, e4.Cause(), 1, "collect non-nil erros only")
	require.NoError(t, serr.Collect(e3, nil, nil), "nil if all errors are nil")
}
