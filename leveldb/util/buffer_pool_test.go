// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package util

import "testing"

// The last pool must not return buffers significantly larger than requested.
// The block cache accounts for memory by buffer capacity, not length.
func TestBufferPoolLastPoolDoesNotHandOutOversizedBuffers(t *testing.T) {
	p := NewBufferPool(4096)
	p.Put(make([]byte, 10<<20))
	const n = 64 << 10
	b := p.Get(n)
	if len(b) != n {
		t.Fatalf("Get(%d) returned len %d", n, len(b))
	}
	if cap(b) > 2*n {
		t.Fatalf("Get(%d) returned a buffer of cap %d, more than twice the request", n, cap(b))
	}
}
