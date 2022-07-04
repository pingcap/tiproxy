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

package sync2

import (
	"errors"
	"sync"
)

var (
	ErrToggleNotPrepared = errors.New("not prepared")
)

type Toggle struct {
	data     [2]interface{}
	idx      int32
	prepared bool
	lock     sync.RWMutex
}

func NewToggle(o interface{}) *Toggle {
	return &Toggle{
		data: [2]interface{}{o},
	}
}

func (t *Toggle) Current() interface{} {
	t.lock.RLock()
	ret := t.data[t.idx]
	t.lock.RUnlock()
	return ret
}

func (t *Toggle) SwapOther(o interface{}) interface{} {
	t.lock.Lock()
	defer t.lock.Unlock()

	tidx := toggleIdx(t.idx)
	origin := t.data[tidx]
	t.data[tidx] = o
	t.prepared = true
	return origin
}

func (t *Toggle) Toggle() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	currIdx := t.idx
	if !t.prepared {
		return ErrToggleNotPrepared
	}

	t.idx = toggleIdx(currIdx)
	t.prepared = false
	return nil
}

func toggleIdx(idx int32) int32 {
	return (idx + 1) % 2
}
