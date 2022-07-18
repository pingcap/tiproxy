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
	"runtime"
)

const defaultStackDepth = 48

var (
	_ error         = &Error{}
	_ fmt.Formatter = &Error{}
)

type Error struct {
	err   error
	isFmt bool
	trace stacktrace
}

func New(text string) *Error {
	return &Error{err: errors.New(text)}
}

func Errorf(format string, args ...any) *Error {
	return &Error{err: fmt.Errorf(format, args...), isFmt: true}
}

// WithStack will wrapping an error with stacktrace, given a default stack depth.
func WithStack(err error) *Error {
	e := &Error{err: err}
	e.withStackDepth(1, defaultStackDepth)
	return e
}

// WithStackDepth is like WithStack, but can specify stack depth.
func WithStackDepth(err error, depth int) *Error {
	e := &Error{err: err}
	e.withStackDepth(1, depth)
	return e
}

func (e *Error) withStackDepth(skip, depth int) {
	e.trace = make(stacktrace, depth)
	runtime.Callers(2+skip, e.trace)
}

// WithStack will capture the current stack and attach it to the error. The old stacktrace will be overrided(if any).
func (e *Error) WithStack() *Error {
	e.withStackDepth(1, defaultStackDepth)
	return e
}

// WithStackDepth is like `e.WithStack`, but can specify stack depth.
func (e *Error) WithStackDepth(depth int) *Error {
	e.withStackDepth(1, depth)
	return e
}

// Format implements `fmt.Formatter`. %+v/%v will contain stacktrace compared to %s.
func (e *Error) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+v", e.err)
			e.trace.Format(st, 'v')
		} else {
			fmt.Fprintf(st, "%v", e.err)
			e.trace.Format(st, 'v')
		}
	case 's':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+s", e.err)
			e.trace.Format(st, 's')
		} else {
			fmt.Fprintf(st, "%s", e.err)
		}
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s", e)
}

func (e *Error) Unwrap() error {
	// for fmt wrapped error, unwrap one more layer
	// such that stacktrace is not an extra layer
	if e.isFmt {
		return errors.Unwrap(e.err)
	}
	return e.err
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	return errors.As(err, target)
}

func Unwrap(err error) error {
	return errors.Unwrap(err)
}
