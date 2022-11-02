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
	"testing"

	serr "github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestWrap(t *testing.T) {
	e1 := serr.New("tt")
	e2 := serr.New("dd")
	e := serr.Wrap(e1, e2)
	require.ErrorIsf(t, e, e1, "equal to the external error")
	require.ErrorAsf(t, e, &e2, "unwrapping to the internal error")

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
