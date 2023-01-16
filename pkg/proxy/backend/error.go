// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/TiProxy/lib/util/errors"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"
)

const (
	connectErrMsg    = "No available TiDB instances, please check TiDB cluster"
	handshakeErrMsg  = "TiProxy fails to connect to TiDB, please check network"
	capabilityErrMsg = "Verify TiDB capability failed, please upgrade TiDB"
)

// UserError is returned to the client.
// err is used to log and userMsg is used to report to the user.
type UserError struct {
	err     error
	userMsg string
}

func WrapUserError(err error, userMsg string) *UserError {
	if err == nil {
		return nil
	}
	if ue, ok := err.(*UserError); ok {
		return ue
	}
	return &UserError{
		err:     err,
		userMsg: userMsg,
	}
}

func (ue *UserError) UserMsg() string {
	return ue.userMsg
}

func (ue *UserError) Unwrap() error {
	return ue.err
}

func (ue *UserError) Error() string {
	return ue.err.Error()
}

// WriteUserError writes an unknown error to the client.
func WriteUserError(clientIO *pnet.PacketIO, err error, lg *zap.Logger) {
	if err == nil {
		return
	}
	var ue *UserError
	if !errors.As(err, &ue) {
		return
	}
	myErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, ue.UserMsg())
	if writeErr := clientIO.WriteErrPacket(myErr); writeErr != nil {
		lg.Error("writing error to client failed", zap.NamedError("mysql_err", err), zap.NamedError("write_err", writeErr))
	}
}
