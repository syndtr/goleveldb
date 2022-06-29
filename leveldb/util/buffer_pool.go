// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package util

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"
)

// BufferPool is a 'buffer pool'.
type BufferPool struct {
	pool     byteSlicePool
	baseline int

	get     uint32
	put     uint32
	less    uint32
	equal   uint32
	greater uint32
	miss    uint32
}

// Get returns buffer with length of n.
func (p *BufferPool) Get(n int) []byte {
	if p == nil {
		return make([]byte, n)
	}
	atomic.AddUint32(&p.get, 1)

	b := p.pool.Get()
	if cap(b) == 0 {
		// If we grabbed nothing, increment the miss stats.
		atomic.AddUint32(&p.miss, 1)

	} else {
		// If there is enough capacity in the bytes grabbed, resize the length
		// to n and return.
		if n < cap(b) {
			atomic.AddUint32(&p.less, 1)
			return b[:n]
		} else if n == cap(b) {
			atomic.AddUint32(&p.equal, 1)
			return b[:n]
		} else if n > cap(b) {
			atomic.AddUint32(&p.greater, 1)
			p.pool.Put(b)
		}
	}
	if n >= p.baseline {
		return make([]byte, n)
	}
	return make([]byte, n, p.baseline)
}

// Put adds given buffer to the pool.
func (p *BufferPool) Put(b []byte) {
	if p == nil {
		return
	}

	atomic.AddUint32(&p.put, 1)
	p.pool.Put(b)
}

func (p *BufferPool) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("BufferPool{G·%d P·%d <·%d =·%d >·%d M·%d}",
		p.get, p.put, p.less, p.equal, p.greater, p.miss)
}

// NewBufferPool creates a new initialized 'buffer pool'.
func NewBufferPool(baseline int) *BufferPool {
	return &BufferPool{baseline: baseline}
}

// byteSlicePool pools byte-slices avoid extra allocations.
type byteSlicePool struct {
	pool sync.Pool
}

func (p *byteSlicePool) Get() (s []byte) {
	if ptr, ok := p.pool.Get().(unsafe.Pointer); ok {
		sh := (*reflect.SliceHeader)(unsafe.Pointer(&s))
		// temporarily set len(s) to 8
		sh.Data = uintptr(ptr)
		sh.Cap = 8
		sh.Len = 8
		// extrat and set actual cap
		sh.Cap = int(binary.BigEndian.Uint64(s))
		sh.Len = 0
	}
	return
}

func (p *byteSlicePool) Put(b []byte) {
	if cap(b) >= 8 {
		// save the slice cap in first 8 bytes
		b = b[:8]
		binary.BigEndian.PutUint64(b, uint64(cap(b)))
		// pools the pointer of the first byte
		p.pool.Put(unsafe.Pointer(&b[0]))
	}
}
