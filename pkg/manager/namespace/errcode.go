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

package namespace

import "github.com/pingcap/TiProxy/lib/util/errors"

var (
	ErrDuplicatedUser      = errors.New("duplicated user")
	ErrInvalidSelectorType = errors.New("invalid selector type")

	ErrNilBreakerName              = errors.New("breaker name nil")
	ErrInvalidFailureRateThreshold = errors.New("invalid FailureRateThreshold")
	ErrInvalidopenStatusDurationMs = errors.New("invalid OpenStatusDurationMs")
	ErrInvalidSqlTimeout           = errors.New("invalid sql timeout")

	ErrInvalidScope = errors.New("invalid scope")
)
