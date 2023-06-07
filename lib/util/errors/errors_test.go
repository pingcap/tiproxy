// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors_test

import (
	"fmt"
	"testing"

	serr "github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestOfficialAPI(t *testing.T) {
	e1 := serr.New("t")
	e2 := fmt.Errorf("%w: f", e1)

	require.True(t, e1 == serr.Unwrap(e2))
	require.True(t, serr.Is(e2, e1))
	require.True(t, serr.As(e2, &e1))
}
