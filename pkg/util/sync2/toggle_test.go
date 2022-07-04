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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Toggle_Current(t *testing.T) {
	currVal := 1
	toggle := NewToggle(currVal)
	assert.Equal(t, currVal, toggle.Current())
}

func Test_Toggle_SwapOther(t *testing.T) {
	currVal := 1
	toggle := NewToggle(currVal)
	swap1 := 2
	ret := toggle.SwapOther(swap1)
	assert.Nil(t, ret)
	assert.Equal(t, currVal, toggle.Current())

	swap2 := 3
	ret = toggle.SwapOther(swap2)
	assert.Equal(t, swap1, ret)
	assert.Equal(t, currVal, toggle.Current())
}

func Test_Toggle_Toggle_Success(t *testing.T) {
	currVal := 1
	toggle := NewToggle(currVal)

	swap := 2
	_ = toggle.SwapOther(swap)
	err := toggle.Toggle()
	assert.NoError(t, err)
	assert.Equal(t, swap, toggle.Current())
}

func Test_Toggle_Toggle_Error_NotPrepared(t *testing.T) {
	currVal := 1
	toggle := NewToggle(currVal)

	err := toggle.Toggle()
	assert.EqualError(t, err, ErrToggleNotPrepared.Error())
}
func BenchmarkToggle(b *testing.B) {
	toggle := NewToggle(1)
	go func() {
		for {
			toggle.SwapOther(2)
			time.Sleep(1 * time.Millisecond)
			_ = toggle.Toggle()
			time.Sleep(1 * time.Millisecond)
		}
	}()
	for i := 0; i < b.N; i++ {
		toggle.Current()
	}
}
