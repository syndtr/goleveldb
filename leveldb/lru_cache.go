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
	"bytes"
)

func lruHash(data []byte) uint32 {
	return Hash(data, 0)
}

type LRUCache struct {
	sync.Mutex

	hash     func([]byte)uint32
	root     lruElem
	table    map[uint32]*lruElem
	capacity int
	size     int
	id       uint64
}

func NewLRUCache(capacity int, hash func([]byte)uint32) Cache {
	c := &LRUCache{hash: lruHash, capacity: capacity}
	if hash != nil {
		c.hash = hash
	}
	c.root.mNext = &c.root
	c.root.mPrev = &c.root
	c.table = make(map[uint32]*lruElem)
	return c
}

func (c *LRUCache) Set(key []byte, value interface{}, charge int) CacheObject {
	c.Lock()
	defer c.Unlock()

	e, created, _, _ := c.lookup(key, true)
	c.size += charge - e.charge
	e.value = value
	e.charge = charge
	if !created {
		e.mRemove()
	}
	// insert to front
	e.mInsert(&c.root)

	// evict least used elem
	c.evict()

	return e.makeObject()
}

func (c *LRUCache) Get(key []byte) (ret CacheObject, ok bool) {
	c.Lock()
	defer c.Unlock()

	e, _, _, _ := c.lookup(key, false)
	if e == nil {
		return
	}

	// bump to front
	e.mRemove()
	e.mInsert(&c.root)

	return e.makeObject(), true 
}

func (c *LRUCache) Delete(key []byte) {
	c.Lock()
	defer c.Unlock()

	e, _, prev, hash := c.lookup(key, false)
	if e == nil {
		return
	}

	// remove from lru list
	e.mRemove()

	// evict elem if no one use it
	if e.ref == 0 {
		if prev == nil {
			if e.tNext == nil {
				delete(c.table, hash)
			} else {
				c.table[hash] = e.tNext
			}
		} else {
			prev.tNext = e.tNext
		}
		c.size -= e.charge
	}
}

func (c *LRUCache) NewId() (id uint64) {
	c.Lock()
	id = c.id
	c.id++
	c.Unlock()
	return
}

func (c *LRUCache) lookup(key []byte, create bool) (e *lruElem, created bool, prev *lruElem, hash uint32) {
	hash = c.hash(key)
	e = c.table[hash]

	// iterate over linked-list
	for ; e != nil; e = e.tNext {
		if bytes.Equal(key, e.key) {
			break
		}
		prev = e
	}

	// create elem
	if e == nil && create {
		e = &lruElem{lru: c, key: key}
		if prev == nil {
			c.table[hash] = e
		} else {
			prev.tNext = e
		}
		created = true
	}
	return
}

func (c *LRUCache) evict() {
	top := c.root.mNext
	for n := c.root.mPrev; c.size > c.capacity && n != top; n = c.root.mPrev {
		// remove from lru list
		n.mRemove()
		// evict elem if no one use it
		n.evict()
	}
}

type lruElem struct {
	lru    *LRUCache

	mNext, mPrev *lruElem
	tNext  *lruElem

	key    []byte
	value  interface{}
	charge int
	ref    uint
}

func (p *lruElem) mInsert(at *lruElem) {
	n := at.mNext
	at.mNext = p
	p.mPrev = at
	p.mNext = n
	n.mPrev = p
	p.ref++
}

func (p *lruElem) mRemove() {
	// only remove if not already removed
	if p.mPrev == nil {
		return
	}

	p.mPrev.mNext = p.mNext
	p.mNext.mPrev = p.mPrev
	p.mPrev = nil
	p.mNext = nil
	p.ref--
}

func (p *lruElem) makeObject() *lruObject {
	p.ref++
	return &lruObject{e: p}
}

func (p *lruElem) evict() {
	if p.ref == 0 {
		lru := p.lru
		hash := lru.hash(p.key)
		x := lru.table[hash]
		if x == p {
			if p.tNext == nil {
				delete(lru.table, hash)
			} else {
				lru.table[hash] = p.tNext
			}
		} else {
			for ; x.tNext == p; x = x.tNext {}
			x.tNext = p.tNext
		}
		lru.size -= p.charge
	}
}

type lruObject struct {
	e    *lruElem
	once bool
}

func (p *lruObject) Value() interface{} {
	return p.e.value
}

func (p *lruObject) Release() {
	p.e.lru.Lock()
	if !p.once {
		p.once = true
		p.e.ref--
		p.e.evict()
	}
	p.e.lru.Unlock()
}

