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
	"fmt"
	"strings"
)

type Capability uint32

// Capability flags. Ref https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html.
const (
	ClientLongPassword Capability = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithDB
	ClientNoSchema
	ClientCompress
	ClientODBC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPSMultiResults
	ClientPluginAuth
	ClientConnectAttrs
	ClientPluginAuthLenencClientData
	ClientCanHandleExpiredPasswords
	ClientSessionTrack
	ClientDeprecateEOF
	ClientOptionalResultsetMetadata
	ClientZstdCompressionAlgorithm
	ClientQueryAttributes
	MultiFactorAuthentication
	ClientCapabilityExtension
	ClientSSLVerifyServerCert
	ClientRememberOptions
)

var capabilityStrings = []struct {
	Cap Capability
	Str string
}{
	{ClientLongPassword, "CLIENT_LONG_PASSWORD"},
	{ClientFoundRows, "CLIENT_FOUND_ROWS"},
	{ClientLongFlag, "CLIENT_LONG_FLAG"},
	{ClientConnectWithDB, "CLIENT_CONNECT_WITH_DB"},
	{ClientNoSchema, "CLIENT_NO_SCHEMA"},
	{ClientCompress, "CLIENT_COMPRESS"},
	{ClientODBC, "CLIENT_ODBC"},
	{ClientLocalFiles, "CLIENT_LOCAL_FILES"},
	{ClientIgnoreSpace, "CLIENT_IGNORE_SPACE"},
	{ClientProtocol41, "CLIENT_PROTOCOL_41"},
	{ClientInteractive, "CLIENT_INTERACTIVE"},
	{ClientSSL, "CLIENT_SSL"},
	{ClientIgnoreSigpipe, "CLIENT_IGNORE_SIGPIPE"},
	{ClientTransactions, "CLIENT_TRANSACTIONS"},
	{ClientReserved, "CLIENT_RESERVED"},
	{ClientSecureConnection, "CLIENT_SECURE_CONNECTION"},
	{ClientMultiStatements, "CLIENT_MULTI_STATEMENTS"},
	{ClientMultiResults, "CLIENT_MULTI_RESULTS"},
	{ClientPSMultiResults, "CLIENT_PS_MULTI_RESULTS"},
	{ClientPluginAuth, "CLIENT_PLUGIN_AUTH"},
	{ClientConnectAttrs, "CLIENT_CONNECT_ATTS"},
	{ClientPluginAuthLenencClientData, "CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA"},
	{ClientCanHandleExpiredPasswords, "CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS"},
	{ClientSessionTrack, "CLIENT_SESSION_TRACK"},
	{ClientDeprecateEOF, "CLIENT_DEPRECATE_EOF"},
	{ClientOptionalResultsetMetadata, "CLIENT_OPTIONAL_RESULTSET_METADATA"},
	{ClientZstdCompressionAlgorithm, "CLIENT_ZSTD_COMPRESSION_ALGORITHM"},
	{ClientQueryAttributes, "CLIENT_QUERY_ATTRIBUTES"},
	{MultiFactorAuthentication, "MULTI_FACTOR_AUTHENTICATION"},
	{ClientCapabilityExtension, "CLIENT_CAPABILITY_EXTENSION"},
	{ClientSSLVerifyServerCert, "CLIENT_SSL_VERIFY_SERVER_CERT"},
	{ClientRememberOptions, "CLIENT_REMEMBER_OPTIONS"},
}

func (f Capability) Uint32() uint32 {
	return uint32(f)
}

func (f Capability) String() string {
	str := &strings.Builder{}
	cnt := 0
	for _, c := range capabilityStrings {
		if f&c.Cap != 0 {
			if cnt > 0 {
				str.WriteByte('|')
			}
			fmt.Fprintf(str, c.Str)
			cnt++
		}
	}
	return str.String()
}

func (f *Capability) MarshalText() ([]byte, error) {
	return []byte(f.String()), nil
}

func (f *Capability) UnmarshalText(o []byte) error {
	var caps Capability
	flags := strings.Split(string(o), "|")
	for _, flag := range flags {
		for _, c := range capabilityStrings {
			if flag == c.Str {
				caps |= c.Cap
			}
		}
	}
	*f = caps
	return nil
}
