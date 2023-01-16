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

package backend

import (
	"testing"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

type respondType int

const (
	responseTypeNone respondType = iota
	responseTypeOK
	responseTypeErr
	responseTypeResultSet
	responseTypeColumn
	responseTypeLoadFile
	responseTypeString
	responseTypeEOF
	responseTypeSwitchRequest
	responseTypePrepareOK
	responseTypeRow
)

// cmdResponseTypes lists all commands and their responses.
var cmdResponseTypes = map[byte][]respondType{
	mysql.ComSleep:            {responseTypeErr},
	mysql.ComQuit:             {responseTypeNone},
	mysql.ComInitDB:           {responseTypeOK, responseTypeErr},
	mysql.ComQuery:            {responseTypeOK, responseTypeErr, responseTypeResultSet, responseTypeLoadFile},
	mysql.ComFieldList:        {responseTypeErr, responseTypeColumn},
	mysql.ComCreateDB:         {responseTypeOK, responseTypeErr},
	mysql.ComDropDB:           {responseTypeOK, responseTypeErr},
	mysql.ComRefresh:          {responseTypeOK, responseTypeErr},
	mysql.ComShutdown:         {responseTypeOK, responseTypeErr},
	mysql.ComStatistics:       {responseTypeString},
	mysql.ComProcessInfo:      {responseTypeErr, responseTypeResultSet},
	mysql.ComConnect:          {responseTypeErr},
	mysql.ComProcessKill:      {responseTypeOK, responseTypeErr},
	mysql.ComDebug:            {responseTypeEOF, responseTypeErr},
	mysql.ComPing:             {responseTypeOK},
	mysql.ComTime:             {responseTypeErr},
	mysql.ComDelayedInsert:    {responseTypeErr},
	mysql.ComChangeUser:       {responseTypeSwitchRequest, responseTypeOK, responseTypeErr},
	mysql.ComBinlogDump:       {responseTypeErr},
	mysql.ComTableDump:        {responseTypeErr},
	mysql.ComConnectOut:       {responseTypeErr},
	mysql.ComRegisterSlave:    {responseTypeErr},
	mysql.ComStmtPrepare:      {responseTypePrepareOK, responseTypeErr},
	mysql.ComStmtExecute:      {responseTypeOK, responseTypeErr, responseTypeResultSet},
	mysql.ComStmtSendLongData: {responseTypeNone},
	mysql.ComStmtClose:        {responseTypeNone},
	mysql.ComStmtReset:        {responseTypeOK, responseTypeErr},
	mysql.ComSetOption:        {responseTypeEOF, responseTypeErr},
	mysql.ComStmtFetch:        {responseTypeRow, responseTypeErr},
	mysql.ComDaemon:           {responseTypeErr},
	mysql.ComBinlogDumpGtid:   {responseTypeErr},
	mysql.ComResetConnection:  {responseTypeOK, responseTypeErr},
	mysql.ComEnd:              {responseTypeErr},
}

// Test forwarding packets between the client and the backend.
func TestForwardCommands(t *testing.T) {
	tc := newTCPConnSuite(t)
	runTest := func(cfgs ...cfgOverrider) {
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.executeCmd(t, nil)
		clean()
	}
	// Test every respond type for every command.
	for cmd, respondTypes := range cmdResponseTypes {
		for _, respondType := range respondTypes {
			for _, capability := range []pnet.Capability{defaultTestBackendCapability &^ pnet.ClientDeprecateEOF, defaultTestBackendCapability | pnet.ClientDeprecateEOF} {
				cfgOvr := func(cfg *testConfig) {
					cfg.clientConfig.cmd = cmd
					cfg.backendConfig.respondType = respondType
					cfg.backendConfig.capability = capability
					cfg.clientConfig.capability = capability
					cfg.proxyConfig.capability = capability
				}
				// Test more variables for some special response types.
				switch respondType {
				case responseTypeColumn:
					for _, columns := range []int{1, 4096} {
						extraCfgOvr := func(cfg *testConfig) {
							cfg.backendConfig.columns = columns
						}
						runTest(cfgOvr, extraCfgOvr)
					}
				case responseTypeRow:
					for _, rows := range []int{0, 1, 3} {
						extraCfgOvr := func(cfg *testConfig) {
							cfg.backendConfig.rows = rows
						}
						runTest(cfgOvr, extraCfgOvr)
					}
				case responseTypePrepareOK:
					for _, columns := range []int{0, 1, 4096} {
						for _, params := range []int{0, 1, 3} {
							extraCfgOvr := func(cfg *testConfig) {
								cfg.backendConfig.columns = columns
								cfg.backendConfig.params = params
							}
							runTest(cfgOvr, extraCfgOvr)
						}
					}
				case responseTypeResultSet:
					for _, columns := range []int{1, 4096} {
						for _, rows := range []int{0, 1, 3} {
							extraCfgOvr := func(cfg *testConfig) {
								cfg.backendConfig.columns = columns
								cfg.backendConfig.rows = rows
							}
							runTest(cfgOvr, extraCfgOvr)
						}
					}
				case responseTypeLoadFile:
					for _, filePkts := range []int{0, 1, 3} {
						extraCfgOvr := func(cfg *testConfig) {
							cfg.clientConfig.filePkts = filePkts
						}
						runTest(cfgOvr, extraCfgOvr)
					}
				default:
					runTest(cfgOvr, cfgOvr)
				}
			}
		}
	}
}

// Test querying directly from the server.
func TestDirectQuery(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfg cfgOverrider
		c   checker
	}{
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.columns = 2
				cfg.backendConfig.rows = 1
				cfg.backendConfig.respondType = responseTypeResultSet
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestBackendCapability &^ pnet.ClientDeprecateEOF
				cfg.proxyConfig.capability = cfg.clientConfig.capability
				cfg.backendConfig.capability = cfg.clientConfig.capability
				cfg.backendConfig.columns = 2
				cfg.backendConfig.rows = 1
				cfg.backendConfig.respondType = responseTypeResultSet
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.respondType = responseTypeErr
			},
			c: func(t *testing.T, ts *testSuite) {
				require.Error(t, ts.mp.err)
				require.NoError(t, ts.mb.err)
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.respondType = responseTypeOK
			},
		},
	}
	for _, test := range tests {
		ts, clean := newTestSuite(t, tc, test.cfg)
		ts.query(t, test.c)
		clean()
	}
}

func TestPreparedStmts(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfgs        []cfgOverrider
		canRedirect bool
	}{
		// prepare
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtPrepare
					cfg.backendConfig.respondType = responseTypePrepareOK
				},
			},
			canRedirect: true,
		},
		// send long data
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
			canRedirect: false,
		},
		// send long data and execute
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeErr
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
			},
			canRedirect: false,
		},
		// execute and fetch
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = mysql.ServerStatusLastRowSend
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeErr
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtFetch
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = mysql.ServerStatusLastRowSend
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtFetch
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = mysql.ServerStatusLastRowSend
				},
			},
			canRedirect: false,
		},
		// send long data and close/reset
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtClose
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtClose
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtReset
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtReset
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: false,
		},
		// execute and close/reset
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtClose
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtReset
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtClose
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtReset
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: false,
		},
		// reset connection and change user
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComResetConnection
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComChangeUser
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
			canRedirect: true,
		},
	}

	for _, test := range tests {
		ts, clean := newTestSuite(t, tc)
		c := func(t *testing.T, ts *testSuite) {
			require.Equal(t, test.canRedirect, ts.mp.cmdProcessor.finishedTxn())
		}
		ts.executeMultiCmd(t, test.cfgs, c)
		clean()
	}
}

// Test whether the session is redirect-able when it has an active transaction.
func TestTxnStatus(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfgs        []cfgOverrider
		canRedirect bool
	}{
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit | mysql.ServerStatusInTrans
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusInTrans
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = 0
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit | mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComChangeUser
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComResetConnection
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit
				},
			},
			canRedirect: true,
		},
	}

	for _, test := range tests {
		ts, clean := newTestSuite(t, tc)
		c := func(t *testing.T, ts *testSuite) {
			require.Equal(t, test.canRedirect, ts.mp.cmdProcessor.finishedTxn())
		}
		ts.executeMultiCmd(t, test.cfgs, c)
		clean()
	}
}

// Test whether the session is redirect-able for mixed prepared statement status and txn status.
func TestMixPrepAndTxnStatus(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfgs        []cfgOverrider
		canRedirect bool
	}{
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusInTrans
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusInTrans | mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = mysql.ServerStatusInTrans | mysql.ServerStatusLastRowSend
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusInTrans | mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = 0
				},
			},
			canRedirect: false,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusInTrans | mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComResetConnection
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit
				},
			},
			canRedirect: true,
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusInTrans | mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = mysql.ServerStatusInTrans | mysql.ServerStatusLastRowSend
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = 0
				},
			},
			canRedirect: true,
		},
	}

	for _, test := range tests {
		ts, clean := newTestSuite(t, tc)
		c := func(t *testing.T, ts *testSuite) {
			require.Equal(t, test.canRedirect, ts.mp.cmdProcessor.finishedTxn())
		}
		ts.executeMultiCmd(t, test.cfgs, c)
		clean()
	}
}

// Test that the BEGIN statement will be held when the session is waiting for redirection.
func TestHoldRequest(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfgs        []cfgOverrider
		holdRequest bool
	}{
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit | mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.clientConfig.sql = "begin"
					cfg.proxyConfig.waitRedirect = true
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit
					cfg.backendConfig.loops = 2
				},
			},
			holdRequest: true,
		},
		// not BEGIN
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit | mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.clientConfig.sql = "commit"
					cfg.proxyConfig.waitRedirect = true
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit
				},
			},
			holdRequest: false,
		},
		// not COM_QUERY
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit | mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit
				},
			},
			holdRequest: false,
		},
		// cursor exists
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit | mysql.ServerStatusInTrans
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = mysql.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = mysql.ComQuery
					cfg.clientConfig.sql = "begin"
					cfg.proxyConfig.waitRedirect = true
					cfg.backendConfig.respondType = responseTypeOK
					cfg.backendConfig.status = mysql.ServerStatusAutocommit | mysql.ServerStatusInTrans
				},
			},
			holdRequest: false,
		},
	}

	for _, test := range tests {
		ts, clean := newTestSuite(t, tc)
		c := func(t *testing.T, ts *testSuite) {
			require.Equal(t, test.holdRequest, ts.mp.holdRequest)
		}
		ts.executeMultiCmd(t, test.cfgs, c)
		clean()
	}
}

func TestBeginStmt(t *testing.T) {
	tests := []struct {
		stmt    string
		isBegin bool
	}{
		{
			stmt:    "begin",
			isBegin: true,
		},
		{
			stmt:    "BEGIN",
			isBegin: true,
		},
		{
			stmt:    "begin optimistic as of timestamp now()",
			isBegin: true,
		},
		{
			stmt:    "    begin",
			isBegin: true,
		},
		{
			stmt:    "start transaction",
			isBegin: true,
		},
		{
			stmt:    "START transaction",
			isBegin: true,
		},
		{
			stmt:    "start transaction with consistent snapshot",
			isBegin: true,
		},
		{
			stmt:    "begin; select 1",
			isBegin: true,
		},
		{
			stmt:    "/*+ some_hint */begin",
			isBegin: true,
		},
		{
			stmt:    "commit",
			isBegin: false,
		},
		{
			stmt:    "select 1; begin",
			isBegin: false,
		},
	}
	for _, test := range tests {
		require.Equal(t, test.isBegin, isBeginStmt(test.stmt))
	}
}

// Test forwarding multi-statements works well.
func TestMultiStmt(t *testing.T) {
	tc := newTCPConnSuite(t)

	// COM_STMT_PREPARE don't support multiple statements, so we only test COM_QUERY.
	cfgs := []cfgOverrider{
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = mysql.ComQuery
			cfg.backendConfig.respondType = responseTypeOK
			cfg.backendConfig.stmtNum = 2
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = mysql.ComQuery
			cfg.backendConfig.respondType = responseTypeResultSet
			cfg.backendConfig.columns = 1
			cfg.backendConfig.stmtNum = 2
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = mysql.ComQuery
			cfg.backendConfig.respondType = responseTypeResultSet
			cfg.backendConfig.columns = 1
			cfg.backendConfig.rows = 1
			cfg.backendConfig.stmtNum = 2
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = mysql.ComQuery
			cfg.backendConfig.respondType = responseTypeResultSet
			cfg.backendConfig.columns = 1
			cfg.backendConfig.stmtNum = 3
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = mysql.ComQuery
			cfg.backendConfig.respondType = responseTypeLoadFile
			cfg.backendConfig.stmtNum = 2
		},
	}

	for _, cfg := range cfgs {
		ts, clean := newTestSuite(t, tc, cfg)
		ts.executeCmd(t, nil)
		clean()
	}
}

// Test that the proxy won't hang or panic when a network error happens.
func TestNetworkError(t *testing.T) {
	tc := newTCPConnSuite(t)

	clientExitCfg := func(cfg *testConfig) {
		cfg.clientConfig.abnormalExit = true
	}
	backendExitCfg := func(cfg *testConfig) {
		cfg.backendConfig.abnormalExit = true
	}
	clientErrChecker := func(t *testing.T, ts *testSuite) {
		require.True(t, pnet.IsDisconnectError(ts.mp.err))
		require.True(t, pnet.IsDisconnectError(ts.mc.err))
		require.NotNil(t, ts.mp.err.(*UserError))
	}
	backendErrChecker := func(t *testing.T, ts *testSuite) {
		require.True(t, pnet.IsDisconnectError(ts.mp.err))
		require.True(t, pnet.IsDisconnectError(ts.mb.err))
	}
	proxyErrChecker := func(t *testing.T, ts *testSuite) {
		require.True(t, pnet.IsDisconnectError(ts.mp.err))
	}

	ts, clean := newTestSuite(t, tc, clientExitCfg)
	ts.authenticateFirstTime(t, backendErrChecker)
	clean()

	ts, clean = newTestSuite(t, tc, backendExitCfg)
	ts.authenticateFirstTime(t, clientErrChecker)
	clean()

	ts, clean = newTestSuite(t, tc, backendExitCfg)
	ts.authenticateSecondTime(t, proxyErrChecker)
	clean()

	ts, clean = newTestSuite(t, tc, clientExitCfg)
	ts.executeCmd(t, backendErrChecker)
	clean()

	ts, clean = newTestSuite(t, tc, clientExitCfg)
	ts.executeCmd(t, backendErrChecker)
	clean()

	ts, clean = newTestSuite(t, tc, backendExitCfg)
	ts.query(t, proxyErrChecker)
	clean()
}
