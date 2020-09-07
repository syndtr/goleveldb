// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync"
)

// simpleBufferPool is a buffer pool based on sync.Pool, used in table compaction process.
type simpleBufferPool struct {
	pool sync.Pool
}

// Get returns buffer with length of n.
func (p *simpleBufferPool) Get(n int) []byte {
	if p == nil {
		return make([]byte, n)
	}
	if buf := p.pool.Get(); buf != nil {
		buf := buf.([]byte)
		if cap(buf) >= n {
			return buf[:n]
		}
	}
	return make([]byte, n)
}

// Put puts back the given buffer.
func (p *simpleBufferPool) Put(buf []byte) {
	if p == nil {
		return
	}
	p.pool.Put(buf)
}
