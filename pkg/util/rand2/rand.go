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
	"sync"
)

type Rand struct {
	sync.Mutex
	stdRand *rand.Rand
}

func New(src rand.Source) *Rand {
	return &Rand{
		stdRand: rand.New(src),
	}
}

func (r *Rand) Int63() int64 {
	r.Lock()
	ret := r.stdRand.Int63()
	r.Unlock()
	return ret
}

func (r *Rand) Uint32() uint32 {
	r.Lock()
	ret := r.stdRand.Uint32()
	r.Unlock()
	return ret

}

func (r *Rand) Uint64() uint64 {
	r.Lock()
	ret := r.stdRand.Uint64()
	r.Unlock()
	return ret
}

func (r *Rand) Int31() int32 {
	r.Lock()
	ret := r.stdRand.Int31()
	r.Unlock()
	return ret
}

func (r *Rand) Int() int {
	r.Lock()
	ret := r.stdRand.Int()
	r.Unlock()
	return ret
}

func (r *Rand) Int63n(n int64) int64 {
	r.Lock()
	ret := r.stdRand.Int63n(n)
	r.Unlock()
	return ret
}

func (r *Rand) Int31n(n int32) int32 {
	r.Lock()
	ret := r.stdRand.Int31n(n)
	r.Unlock()
	return ret
}

func (r *Rand) Intn(n int) int {
	r.Lock()
	ret := r.stdRand.Intn(n)
	r.Unlock()
	return ret
}

func (r *Rand) Float64() float64 {
	r.Lock()
	ret := r.stdRand.Float64()
	r.Unlock()
	return ret
}

func (r *Rand) Float32() float32 {
	r.Lock()
	ret := r.stdRand.Float32()
	r.Unlock()
	return ret
}
