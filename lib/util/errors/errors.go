// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"errors"
	"fmt"
)

func New(text string) error {
	return errors.New(text)
}

func Errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

func Unwrap(err error) error {
	return errors.Unwrap(err)
}
