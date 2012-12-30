// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This LevelDB Go implementation is based on LevelDB C++ implementation.
// Which contains the following header:
//   Copyright (c) 2011 The LevelDB Authors. All rights reserved.
//   Use of this source code is governed by a BSD-style license that can be
//   found in the LEVELDBCPP_LICENSE file. See the LEVELDBCPP_AUTHORS file
//   for names of contributors.

package leveldb

import (
	"sync"
	"sync/atomic"
)

type LRUCache struct {
	sync.Mutex

	root     lruElem
	table    map[uint64]*lruNs
	capacity int
	size     int
}

func NewLRUCache(capacity int) *LRUCache {
	c := &LRUCache{
		table:    make(map[uint64]*lruNs),
		capacity: capacity,
	}
	c.root.mNext = &c.root
	c.root.mPrev = &c.root
	return c
}

func (c *LRUCache) GetNamespace(id uint64) CacheNamespace {
	c.Lock()
	defer c.Unlock()

	if p, ok := c.table[id]; ok {
		return p
	}

	p := &lruNs{
		lru:   c,
		table: make(map[uint64]*lruElem),
	}
	c.table[id] = p
	return p
}

func (c *LRUCache) Purge(finalizer func()) {
	c.Lock()
	top := &c.root
	for e := c.root.mPrev; e != top; {
		e.deleted = true
		e.mRemove()
		e.delFinalizer = finalizer
		e.evict()
		e = c.root.mPrev
	}
	c.Unlock()
}

func (c *LRUCache) evict() {
	top := c.root.mNext
	for e := c.root.mPrev; c.size > c.capacity && e != top; {
		e.mRemove()
		e.evict()
		e = c.root.mPrev
	}
}

type lruNs struct {
	lru   *LRUCache
	table map[uint64]*lruElem
}

func (p *lruNs) Set(key uint64, value interface{}, charge int, finalizer func()) CacheObject {
	lru := p.lru
	lru.Lock()

	e, ok := p.table[key]
	if !ok {
		e = &lruElem{ns: p, key: key}
		p.table[key] = e
	} else {
		e.mRemove()
		lru.size -= e.charge
	}
	e.value = value
	e.charge = charge
	e.deleted = false
	e.setFinalizer = finalizer
	e.delFinalizer = nil
	e.mInsert(&lru.root)

	lru.size += charge
	lru.evict()
	lru.Unlock()

	return e.makeObject()
}

func (p *lruNs) Get(key uint64) (obj CacheObject, ok bool) {
	lru := p.lru
	lru.Lock()

	e, ok := p.table[key]
	if !ok {
		lru.Unlock()
		return
	}

	if !e.deleted {
		// bump to front
		e.mRemove()
		e.mInsert(&lru.root)
	}

	lru.Unlock()
	obj = e.makeObject()
	return
}

func (p *lruNs) Delete(key uint64, finalizer func()) bool {
	lru := p.lru
	lru.Lock()

	e, ok := p.table[key]
	if !ok {
		lru.Unlock()
		if finalizer != nil {
			finalizer()
		}
		return false
	}

	if e.deleted {
		lru.Unlock()
		return false
	}

	e.deleted = true
	e.mRemove()
	e.delFinalizer = finalizer
	e.evict()

	lru.Unlock()
	return true
}

func (p *lruNs) Purge(finalizer func()) {
	p.lru.Lock()
	for _, e := range p.table {
		if e.deleted {
			continue
		}
		e.mRemove()
		e.delFinalizer = finalizer
		e.evict()
	}
	p.lru.Unlock()
}

type lruElem struct {
	ns *lruNs

	mNext, mPrev *lruElem

	key          uint64
	value        interface{}
	charge       int
	ref          uint
	deleted      bool
	setFinalizer func()
	delFinalizer func()
}

func (e *lruElem) mInsert(at *lruElem) {
	n := at.mNext
	at.mNext = e
	e.mPrev = at
	e.mNext = n
	n.mPrev = e
	e.ref++
}

func (e *lruElem) mRemove() {
	// only remove if not already removed
	if e.mPrev == nil {
		return
	}

	e.mPrev.mNext = e.mNext
	e.mNext.mPrev = e.mPrev
	e.mPrev = nil
	e.mNext = nil
	e.ref--
}

func (e *lruElem) evict() {
	if e.ref != 0 {
		return
	}

	ns := e.ns

	// remove elem
	delete(ns.table, e.key)
	ns.lru.size -= e.charge

	// execute finalizer
	if e.setFinalizer != nil {
		e.setFinalizer()
		e.setFinalizer = nil
	}
	if e.delFinalizer != nil {
		e.delFinalizer()
		e.delFinalizer = nil
	}
}

func (e *lruElem) makeObject() (obj *lruObject) {
	e.ref++
	return &lruObject{e: e}
}

type lruObject struct {
	e    *lruElem
	once uint32
}

func (p *lruObject) Value() interface{} {
	return p.e.value
}

func (p *lruObject) Release() {
	if !atomic.CompareAndSwapUint32(&p.once, 0, 1) {
		return
	}

	e := p.e
	lru := e.ns.lru
	lru.Lock()
	e.ref--
	e.evict()
	lru.Unlock()
}
