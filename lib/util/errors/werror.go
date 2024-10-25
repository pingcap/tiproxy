// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
			fmt.Fprintf(st, "%+v: %+v", e.uerr, e.cerr)
		} else {
			fmt.Fprintf(st, "%v: %v", e.uerr, e.cerr)
		}
	case 's':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+s: %+s", e.uerr, e.cerr)
		} else {
			fmt.Fprintf(st, "%s: %s", e.uerr, e.cerr)
		}
	}
}

func (e *WError) Error() string {
	return fmt.Sprintf("%s", e)
}

func (e *WError) Is(s error) bool {
	r := errors.Is(e.cerr, s)
	if r {
		return r
	}
	return errors.Is(e.uerr, s)
}

func (e *WError) Unwrap() error {
	return e.cerr
}

// Wrap is used to wrapping unknown errors. A typical example is that:
// 1. have a function `ReadMyConfig()`
// 2. it got errors returned from external libraries
// 3. you want to wrap these errors, expect `Unwrap(err) == ErrReadMyConfig && Is(err, ErrReadMyConfig) && Is(err, ErrExternalErrors)`.
// 4. then you are finding `err := Wrap(ErrReadMyConfig, ErrExternalErrors)`
// Note that wrap nil error will get nil error.
func Wrap(cerr error, uerr error) error {
	if cerr == nil {
		return nil
	}
	if uerr == nil {
		return cerr
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
