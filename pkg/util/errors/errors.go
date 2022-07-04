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
	"reflect"

	gomysql "github.com/siddontang/go-mysql/mysql"
)

// copied from errors.Is(), but replace Unwrap() with Cause()
func Is(err, target error) bool {
	if target == nil {
		return err == target
	}

	isComparable := reflect.TypeOf(target).Comparable()
	for {
		if isComparable && err == target {
			return true
		}
		if x, ok := err.(interface{ Is(error) bool }); ok && x.Is(target) {
			return true
		}
		// TODO: consider supporing target.Is(err). This would allow
		// user-definable predicates, but also may allow for coping with sloppy
		// APIs, thereby making it easier to get away with them.
		if err = Cause(err); err == nil {
			return false
		}
	}
}

func CheckAndGetMyError(err error) (*gomysql.MyError, bool) {
	if err == nil {
		return nil, false
	}

	for {
		if err1, ok := err.(*gomysql.MyError); ok {
			return err1, true
		}
		if err = Cause(err); err == nil {
			return nil, false
		}
	}
}

func Cause(err error) error {
	u, ok := err.(interface {
		Cause() error
	})
	if !ok {
		return nil
	}
	return u.Cause()
}
