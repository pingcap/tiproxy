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
