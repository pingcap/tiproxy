// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
