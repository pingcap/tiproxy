// Copyright 2020 Ipalfish, Inc.
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

package errors

import (
	stderrors "errors"
	"testing"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

func TestIs(t *testing.T) {
	badConn := mysql.ErrBadConn
	err := errors.Wrapf(badConn, "same error type")
	assert.True(t, Is(err, badConn))
}

func TestStdIs(t *testing.T) {
	badConn := mysql.ErrBadConn
	err := errors.Wrapf(badConn, "another error type")
	assert.False(t, stderrors.Is(err, badConn))
}

func TestCheckAndGetMyError_True(t *testing.T) {
	myErr := mysql.NewError(1105, "unknown")
	err, is := CheckAndGetMyError(myErr)
	assert.True(t, is)
	assert.NotNil(t, err)
}

func TestCheckAndGetMyError_False(t *testing.T) {
	myErr := errors.New("not a myError")
	err, is := CheckAndGetMyError(myErr)
	assert.False(t, is)
	assert.Nil(t, err)
}

func TestCheckAndGetMyError_Cause_True(t *testing.T) {
	myErr := errors.Wrapf(mysql.NewError(1105, "unknown"), "wrap error")
	err, is := CheckAndGetMyError(myErr)
	assert.True(t, is)
	assert.NotNil(t, err)
}
