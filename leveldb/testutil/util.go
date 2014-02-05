// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package testutil

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

var cmp = comparer.DefaultComparer

func BytesSeparator(a, b []byte) []byte {
	if bytes.Equal(a, b) {
		return b
	}
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for ; i < n && (a[i] == b[i]); i++ {
	}
	x := append([]byte{}, a[:i]...)
	if i < n {
		if c := a[i] + 1; c < b[i] {
			return append(x, c)
		}
		x = append(x, a[i])
		i++
	}
	for ; i < len(a); i++ {
		if c := a[i]; c < 0xff {
			return append(x, c+1)
		} else {
			x = append(x, c)
		}
	}
	if len(b) > i && b[i] > 0 {
		return append(x, b[i]-1)
	}
	return append(x, 'x')
}

func BytesAfter(b []byte) []byte {
	var x []byte
	for _, c := range b {
		if c < 0xff {
			return append(x, c+1)
		} else {
			x = append(x, c)
		}
	}
	return append(x, 'x')
}

func makeRand(rnd *rand.Rand) (rnd_ *rand.Rand, seed int64) {
	rnd_ = rnd
	if rnd_ == nil {
		seed = time.Now().UnixNano()
		rnd_ = rand.New(rand.NewSource(seed))
	}
	return
}

func RandomIndex(rnd *rand.Rand, n, round int, fn func(i int)) (seed int64) {
	rnd, seed = makeRand(rnd)
	for x := 0; x < round; x++ {
		fn(rnd.Intn(n))
	}
	return
}

func ShuffledIndex(rnd *rand.Rand, n, round int, fn func(i int)) (seed int64) {
	rnd, seed = makeRand(rnd)
	for x := 0; x < round; x++ {
		for _, i := range rnd.Perm(n) {
			fn(i)
		}
	}
	return
}

func RandomRange(rnd *rand.Rand, n, round int, fn func(start, limit int)) (seed int64) {
	rnd, seed = makeRand(rnd)
	for x := 0; x < round; x++ {
		start := rnd.Intn(n)
		length := 0
		if j := n - start; j > 0 {
			length = rnd.Intn(j)
		}
		fn(start, start+length)
	}
	return
}
