// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync"
	"unsafe"
)

type lruNode struct {
	n     *Node
	h     *Handle
	ban   bool
	lruNS byte

	next, prev *lruNode
}

func (n *lruNode) insert(at *lruNode) {
	x := at.next
	at.next = n
	n.prev = at
	n.next = x
	x.prev = n
}

func (n *lruNode) remove() {
	if n.prev != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		n.prev = nil
		n.next = nil
	} else {
		panic("BUG: removing removed node")
	}
}

type SharedLRU struct {
	mu       sync.Mutex
	capacity int
	used     int
	recent   lruNode
}

func (r *SharedLRU) reset() {
	r.recent.next = &r.recent
	r.recent.prev = &r.recent
	r.used = 0
}

func (r *SharedLRU) _capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}

func (r *SharedLRU) setCapacity(capacity int) {
	var evicted []*lruNode

	r.mu.Lock()
	r.capacity = capacity
	for r.used > r.capacity {
		rn := r.recent.prev
		if rn == nil {
			panic("BUG: invalid LRU used or capacity counter")
		}
		rn.remove()
		rn.n.CacheData = nil
		r.used -= rn.n.Size()
		evicted = append(evicted, rn)
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *SharedLRU) promote(n *Node, lruNS byte) {
	var evicted []*lruNode

	r.mu.Lock()
	if n.CacheData == nil {
		if n.Size() <= r.capacity {
			rn := &lruNode{n: n, h: n.GetHandle(), lruNS: lruNS}
			rn.insert(&r.recent)
			n.CacheData = unsafe.Pointer(rn)
			r.used += n.Size()

			for r.used > r.capacity {
				rn := r.recent.prev
				if rn == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				rn.remove()
				rn.n.CacheData = nil
				r.used -= rn.n.Size()
				evicted = append(evicted, rn)
			}
		}
	} else {
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.insert(&r.recent)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *SharedLRU) ban(n *Node) {
	r.mu.Lock()
	if n.CacheData == nil {
		n.CacheData = unsafe.Pointer(&lruNode{n: n, ban: true})
	} else {
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.ban = true
			r.used -= rn.n.Size()
			r.mu.Unlock()

			rn.h.Release()
			rn.h = nil
			return
		}
	}
	r.mu.Unlock()
}

func (r *SharedLRU) evict(n *Node) {
	r.mu.Lock()
	rn := (*lruNode)(n.CacheData)
	if rn == nil || rn.ban {
		r.mu.Unlock()
		return
	}
	n.CacheData = nil
	r.mu.Unlock()

	rn.h.Release()
}

//nodeNS optional.
func (r *SharedLRU) evictMulti(lruNS byte, nodeNS *uint64) {
	var evicted []*lruNode

	r.mu.Lock()
	for e := r.recent.prev; e != &r.recent; {
		rn := e
		e = e.prev
		if rn.lruNS != lruNS || (nodeNS != nil && rn.n.NS() != *nodeNS) {
			continue
		}
		rn.remove()
		rn.n.CacheData = nil
		r.used -= rn.n.Size()
		evicted = append(evicted, rn)
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

type lru struct {
	lruNS byte
	s     *SharedLRU
}

func (r *lru) Capacity() int {
	return r.s._capacity()
}

func (r *lru) SetCapacity(capacity int) {
	r.s.setCapacity(capacity)
}

func (r *lru) Promote(n *Node) {
	r.s.promote(n, r.lruNS)
}

func (r *lru) Ban(n *Node) {
	r.s.ban(n)
}

func (r *lru) Evict(n *Node) {
	r.s.evict(n)
}

func (r *lru) EvictNS(ns uint64) {
	r.s.evictMulti(r.lruNS, &ns)
}

func (r *lru) EvictAll() {
	r.s.evictMulti(r.lruNS, nil)
}

func (r *lru) Close() error {
	return nil
}

// NewSharedLRU creates a new shared lru.
func NewSharedLRU(capacity int) *SharedLRU {
	r := &SharedLRU{capacity: capacity}
	r.reset()
	return r
}

// NewLRU create a new namespaced LRU-cache.
func NewNamespaceLRU(ns byte, s *SharedLRU) Cacher {
	return &lru{ns, s}
}

// NewLRU create a new LRU-cache.
func NewLRU(capacity int) Cacher {
	s := NewSharedLRU(capacity)
	return NewNamespaceLRU(0, s)
}
