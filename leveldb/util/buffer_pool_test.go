// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package util

import "testing"

func BenchmarkBufferPool(b *testing.B) {
	const n = 100
	pool := NewBufferPool(n)

	for i := 0; i < b.N; i++ {
		buf := pool.Get(n)
		pool.Put(buf)
	}
}
