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

package net

import (
	"encoding"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapability(t *testing.T) {
	caps := ClientCanHandleExpiredPasswords

	var capStringer fmt.Stringer = caps
	require.Equal(t, "CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS", capStringer.String())

	caps |= ClientSSL
	require.Equal(t, "CLIENT_SSL|CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS", caps.String())

	var capMarshaler encoding.TextMarshaler = &caps
	capBytes, err := capMarshaler.MarshalText()
	require.NoError(t, err)
	require.Equal(t, caps.String(), string(capBytes))

	var newcaps Capability
	var newcapsUnmarshaler encoding.TextUnmarshaler = &newcaps
	require.NoError(t, newcapsUnmarshaler.UnmarshalText(capBytes))
	require.Equal(t, caps.String(), newcaps.String())

	require.Equal(t, uint32(caps), caps.Uint32())
}
