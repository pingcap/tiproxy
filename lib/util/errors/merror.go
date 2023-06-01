// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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

func (e *MError) Is(s error) bool {
	is := errors.Is(e.cerr, s)
	for _, e := range e.uerr {
		is = is || errors.Is(e, s)
		if is {
			break
		}
	}
	return is
}

func (e *MError) Cause() []error {
	return e.uerr
}

// Collect is used to collect multiple errors. `Unwrap` is noop and `Is(err, ErrMine) == true`. While `As(err, underlyingError)` do not work, you can still get underlying errors by `MError.Cause`.
func Collect(cerr error, uerr ...error) error {
	n := 0
	for _, e := range uerr {
		if e != nil {
			uerr[n] = e
			n++
		}
	}
	uerr = uerr[:n]
	if len(uerr) == 0 {
		return nil
	}
	return &MError{
		uerr: uerr,
		cerr: cerr,
	}
}
