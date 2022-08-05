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

	"github.com/pingcap/tidb/parser/mysql"
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
)

// cmdResponseTypes lists simple commands and their responses.
var cmdResponseTypes = map[byte][]respondType{
	mysql.ComSleep:         {responseTypeErr},
	mysql.ComQuit:          {responseTypeOK, responseTypeNone},
	mysql.ComInitDB:        {responseTypeOK, responseTypeErr},
	mysql.ComQuery:         {responseTypeOK, responseTypeErr, responseTypeResultSet, responseTypeLoadFile},
	mysql.ComFieldList:     {responseTypeErr, responseTypeColumn},
	mysql.ComCreateDB:      {responseTypeOK, responseTypeErr},
	mysql.ComDropDB:        {responseTypeOK, responseTypeErr},
	mysql.ComRefresh:       {responseTypeOK, responseTypeErr},
	mysql.ComShutdown:      {responseTypeOK, responseTypeErr},
	mysql.ComStatistics:    {responseTypeString},
	mysql.ComProcessInfo:   {responseTypeErr, responseTypeResultSet},
	mysql.ComConnect:       {responseTypeErr},
	mysql.ComProcessKill:   {responseTypeOK, responseTypeErr},
	mysql.ComDebug:         {responseTypeEOF, responseTypeErr},
	mysql.ComPing:          {responseTypeOK},
	mysql.ComTime:          {responseTypeErr},
	mysql.ComDelayedInsert: {responseTypeErr},
	mysql.ComChangeUser:    {responseTypeSwitchRequest, responseTypeOK, responseTypeErr},
	mysql.ComBinlogDump:    {responseTypeErr},
	mysql.ComTableDump:     {responseTypeErr},
	mysql.ComConnectOut:    {responseTypeErr},
	mysql.ComRegisterSlave: {responseTypeErr},
	//mysql.ComStmtPrepare:      {responseTypeOK, responseTypeErr},
	//mysql.ComStmtExecute:      {responseTypeOK, responseTypeErr},
	//mysql.ComStmtSendLongData: {responseTypeOK, responseTypeErr},
	//mysql.ComStmtClose:        {responseTypeOK, responseTypeErr},
	//mysql.ComStmtReset:        {responseTypeOK, responseTypeErr},
	mysql.ComSetOption: {responseTypeEOF, responseTypeErr},
	//mysql.ComStmtFetch:        {responseTypeOK, responseTypeErr},
	mysql.ComDaemon:          {responseTypeErr},
	mysql.ComBinlogDumpGtid:  {responseTypeErr},
	mysql.ComResetConnection: {responseTypeOK, responseTypeErr},
	mysql.ComEnd:             {responseTypeErr},
}

func TestSimpleCommands(t *testing.T) {
	tc := newTCPConnSuite(t)
	runTest := func(cfgs ...cfgOverrider) {
		ts, clean := newTestSuite(t, tc, cfgs...)
		println(ts.mc.cmd, ts.mb.respondType, ts.mb.columns, ts.mb.rows)
		// Only verify that it won't hang or report errors.
		ts.executeCmd(t)
		clean()
	}
	// Test every respond type for every command.
	for cmd, respondTypes := range cmdResponseTypes {
		for _, respondType := range respondTypes {
			cfgOvr := func(cfg *testConfig) {
				cfg.clientConfig.cmd = cmd
				cfg.backendConfig.respondType = respondType
			}
			// Test more variables for some special response types.
			switch respondType {
			case responseTypeColumn:
				for _, columns := range []int{1, 3} {
					extraCfgOvr := func(cfg *testConfig) {
						cfg.backendConfig.columns = columns
					}
					runTest(cfgOvr, extraCfgOvr)
				}
			case responseTypeResultSet:
				for _, columns := range []int{1, 3} {
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
