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
	"errors"
	"fmt"
)

var (
	_ error = &WError{}
)

type WError struct {
	uerr error
	cerr error
}

func (e *WError) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+v: %+v", e.cerr, e.uerr)
		} else {
			fmt.Fprintf(st, "%v: %v", e.cerr, e.uerr)
		}
	case 's':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+s: %+s", e.cerr, e.uerr)
		} else {
			fmt.Fprintf(st, "%s: %s", e.cerr, e.uerr)
		}
	}
}

func (e *WError) Error() string {
	return fmt.Sprintf("%s", e)
}

func (e *WError) Is(s error) bool {
	return errors.Is(e.cerr, s)
}

func (e *WError) Unwrap() error {
	return e.uerr
}

// Wrap is used to wrapping unknown errors. A typical example is that:
// 1. have a function `ReadMyConfig()`
// 2. it got errors returned from external libraries
// 3. you want to wrap these errors, expect `Unwrap(err) == ErrExternalErrors && Is(err, ErrReadMyConfig)`.
// 4. then you are finding `err := Wrap(ErrReadMyConfig, ErrExternalErrors)`
// Note that wrap nil error will get nil error.
func Wrap(cerr error, uerr error) error {
	if cerr == nil {
		return nil
	}
	return &WError{
		uerr: uerr,
		cerr: cerr,
	}
}

// Wrapf is like Wrap, with the underlying error being the result of `fmt.Errorf()`
func Wrapf(cerr error, msg string, args ...interface{}) error {
	if cerr == nil {
		return nil
	}
	return &WError{
		uerr: fmt.Errorf(msg, args...),
		cerr: cerr,
	}
}
