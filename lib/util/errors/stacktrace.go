// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
)

var (
	_ fmt.Formatter = stacktrace(nil)
)

// formatFrame will print frame according to fmt.Formatter states. It is extracted to keep `stacktrace.Format` clean.
func formatFrame(s fmt.State, fr runtime.Frame, verb rune) {
	fn := fr.Function
	if fn == "" {
		fn = "unknown"
	}
	switch verb {
	case 'v':
		fallthrough
	case 's':
		io.WriteString(s, fn)
		io.WriteString(s, "\n\t")
		io.WriteString(s, fr.File)
		if s.Flag('+') {
			io.WriteString(s, ":")
			formatFrame(s, fr, 'd')
		}
	case 'd':
		io.WriteString(s, strconv.Itoa(fr.Line))
	case 'n':
		i := strings.LastIndex(fn, "/")
		fn = fn[i+1:]
		i = strings.Index(fn, ".")
		io.WriteString(s, fn[i+1:])
	}
}

// stacktrace only stores the pointers, information will be queried as need.
type stacktrace []uintptr

// Format formats the stack of Frames according to the fmt.Formatter interface.
func (st stacktrace) Format(s fmt.State, verb rune) {
	frames := runtime.CallersFrames(st)
	for {
		fr, more := frames.Next()

		io.WriteString(s, "\n")
		formatFrame(s, fr, verb)

		if !more {
			break
		}
	}
}
