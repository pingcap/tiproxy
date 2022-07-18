// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errors_test

import (
	gerr "errors"
	"fmt"
	"testing"

	serr "github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/stretchr/testify/require"
)

func TestOfficialAPI(t *testing.T) {
	e1 := gerr.New("t")
	e2 := fmt.Errorf("%w: f", e1)

	require.True(t, e1 == serr.Unwrap(e2))
	require.True(t, serr.Is(e2, e1))
	require.True(t, serr.As(e2, &e1))
}

func TestStacktrace(t *testing.T) {
	e := serr.New("tt").WithStack()
	require.Equal(t, fmt.Sprintf("%s", e), "tt")
	require.Contains(t, fmt.Sprintf("%+v", e), t.Name(), "stacktrace must contain test name")
	require.Contains(t, fmt.Sprintf("%v", e), t.Name(), "stacktrace must contain test name")
	require.Contains(t, fmt.Sprintf("%+s", e), t.Name(), "stacktrace must contain test name")
}

func TestUnwrap(t *testing.T) {
	e1 := gerr.New("t")
	e2 := serr.WithStack(e1)
	require.Equal(t, e1, gerr.Unwrap(e2), "unwrapped error should be identical")
	require.ErrorIs(t, e2, e1, "stacktrace does not affect Is")
	require.ErrorAs(t, e2, &e1, "stacktrace does not affect As")

	e3 := serr.Errorf("%w: f", e1)
	require.Equal(t, e1, gerr.Unwrap(e3), "unwrapped error should be identical")
	require.ErrorIs(t, e3, e1, "stacktrace does not affect Is")
	require.ErrorAs(t, e3, &e1, "stacktrace does not affect As")
}
