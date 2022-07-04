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

package rand2

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRand(t *testing.T) {
	src1 := rand.NewSource(1)
	stdRd := rand.New(src1)
	src2 := rand.NewSource(1)
	rd := New(rand.New(src2))

	assert.Equal(t, stdRd.Int63(), rd.Int63())
	assert.Equal(t, stdRd.Uint32(), rd.Uint32())
	assert.Equal(t, stdRd.Uint64(), rd.Uint64())
	assert.Equal(t, stdRd.Int31(), rd.Int31())
	assert.Equal(t, stdRd.Int63n(100), rd.Int63n(100))
	assert.Equal(t, stdRd.Int31n(100), rd.Int31n(100))
	assert.Equal(t, stdRd.Intn(20), rd.Intn(20))
	assert.Equal(t, stdRd.Float64(), rd.Float64())
	assert.Equal(t, stdRd.Float32(), rd.Float32())
}
