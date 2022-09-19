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
	_ error = &MError{}
)

type MError struct {
	cerr error
	uerr []error
}

func (e *MError) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+v:\n", e.cerr)
			for _, ue := range e.uerr {
				fmt.Fprintf(st, "\t%+v", ue)
			}
		} else {
			fmt.Fprintf(st, "%v:\n", e.cerr)
			for _, ue := range e.uerr {
				fmt.Fprintf(st, "\t%v", ue)
			}
		}
	case 's':
		if st.Flag('+') {
			fmt.Fprintf(st, "%+s:\n", e.cerr)
			for _, ue := range e.uerr {
				fmt.Fprintf(st, "\t%+s", ue)
			}
		} else {
			fmt.Fprintf(st, "%s:\n", e.cerr)
			for _, ue := range e.uerr {
				fmt.Fprintf(st, "\t%s", ue)
			}
		}
	}
}

func (e *MError) Error() string {
	return fmt.Sprintf("%s", e)
}

func (e *MError) Unwrap() error {
	return e
}

func (e *MError) Is(s error) bool {
	return errors.Is(e.cerr, s)
}

func (e *MError) Cause() []error {
	return e.uerr
}

// Collect is used to collect multiple errors. `Unwrap` is noop and `Is(err, ErrMine) == true`. While `As(err, underlyingError)` do not work, you can still get underlying errors by `MError.Cause`.
func Collect(cerr error, uerr ...error) error {
	if len(uerr) == 0 {
		return nil
	}
	return &MError{
		uerr: uerr,
		cerr: cerr,
	}
}
