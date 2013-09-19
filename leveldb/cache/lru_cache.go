// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync"
	"sync/atomic"
)

// LRUCache represent a LRU cache state.
type LRUCache struct {
	sync.Mutex

	recent   lruNode
	table    map[uint64]*lruNs
	capacity int
	size     int
}

// NewLRUCache create new initialized LRU cache.
func NewLRUCache(capacity int) *LRUCache {
	c := &LRUCache{
		table:    make(map[uint64]*lruNs),
		capacity: capacity,
	}
	c.recent.rNext = &c.recent
	c.recent.rPrev = &c.recent
	return c
}

// SetCapacity set cache capacity.
func (c *LRUCache) SetCapacity(capacity int) {
	c.Lock()
	c.capacity = capacity
	c.evict()
	c.Unlock()
}

// GetNamespace return namespace object for given id.
func (c *LRUCache) GetNamespace(id uint64) Namespace {
	c.Lock()
	defer c.Unlock()

	if p, ok := c.table[id]; ok {
		return p
	}

	p := &lruNs{
		lru:   c,
		id:    id,
		table: make(map[uint64]*lruNode),
	}
	c.table[id] = p
	return p
}

// Purge purge entire cache.
func (c *LRUCache) Purge(fin func()) {
	c.Lock()
	top := &c.recent
	for n := c.recent.rPrev; n != top; {
		n.state = stRemoved
		n.rRemove()
		n.delfin = fin
		n.evictNB()
		n = c.recent.rPrev
	}
	c.size = 0
	c.Unlock()
}

func (c *LRUCache) Zap() {
	c.Lock()
	for _, ns := range c.table {
		for _, n := range ns.table {
			n.rNext = nil
			n.rPrev = nil
			n.execFin()
		}
		ns.zapped = true
		ns.table = nil
	}
	c.recent.rNext = &c.recent
	c.recent.rPrev = &c.recent
	c.size = 0
	c.table = make(map[uint64]*lruNs)
	c.Unlock()
}

func (c *LRUCache) evict() {
	top := &c.recent
	for n := c.recent.rPrev; c.size > c.capacity && n != top; {
		n.state = stEvicted
		n.rRemove()
		n.evictNB()
		c.size -= n.charge
		n = c.recent.rPrev
	}
}

type lruNs struct {
	lru    *LRUCache
	id     uint64
	table  map[uint64]*lruNode
	zapped bool
}

func (p *lruNs) Get(key uint64, setf SetFunc) (o Object, ok bool) {
	lru := p.lru
	lru.Lock()

	if p.zapped {
		lru.Unlock()
		if setf == nil {
			return
		}

		var value interface{}
		var fin func()
		ok, value, _, fin = setf()
		if ok {
			o = &emptyCacheObj{value, fin}
		}
		return
	}

	n, ok := p.table[key]
	if ok {
		switch n.state {
		case stEvicted:
			// Insert to recent list.
			n.state = stEffective
			n.ref++
			lru.size += n.charge
			lru.evict()
			fallthrough
		case stEffective:
			// Bump to front
			n.rRemove()
			n.rInsert(&lru.recent)
		}
		n.ref++
	} else {
		if setf == nil {
			lru.Unlock()
			return
		}

		var value interface{}
		var charge int
		var fin func()
		ok, value, charge, fin = setf()
		if !ok {
			lru.Unlock()
			return
		}

		n = &lruNode{
			ns:     p,
			key:    key,
			value:  value,
			charge: charge,
			setfin: fin,
			ref:    2,
		}
		p.table[key] = n
		n.rInsert(&lru.recent)

		lru.size += charge
		lru.evict()
	}

	lru.Unlock()
	o = &lruObject{node: n}
	return
}

func (p *lruNs) Delete(key uint64, fin func()) bool {
	lru := p.lru
	lru.Lock()

	if p.zapped {
		lru.Unlock()
		if fin != nil {
			fin()
		}
		return false
	}

	n, ok := p.table[key]
	if !ok {
		lru.Unlock()
		if fin != nil {
			fin()
		}
		return false
	}

	n.delfin = fin
	switch n.state {
	case stRemoved:
		lru.Unlock()
		return false
	case stEffective:
		lru.size -= n.charge
		n.rRemove()
		n.evictNB()
	}
	n.state = stRemoved

	lru.Unlock()
	return true
}

func (p *lruNs) Purge(fin func()) {
	lru := p.lru

	lru.Lock()
	if p.zapped {
		return
	}

	for _, n := range p.table {
		n.delfin = fin
		if n.state == stEffective {
			lru.size -= n.charge
			n.rRemove()
			n.evictNB()
		}
		n.state = stRemoved
	}
	lru.Unlock()
}

func (p *lruNs) Zap() {
	lru := p.lru

	lru.Lock()
	if p.zapped {
		return
	}

	for _, n := range p.table {
		if n.state == stEffective {
			lru.size -= n.charge
			n.rRemove()
		}
		n.state = stRemoved
		n.execFin()
	}
	p.zapped = true
	p.table = nil
	delete(lru.table, p.id)
	lru.Unlock()
}

type state int

const (
	stEffective state = iota
	stEvicted
	stRemoved
)

type lruNode struct {
	ns *lruNs

	rNext, rPrev *lruNode

	key    uint64
	value  interface{}
	charge int
	ref    int
	state  state
	setfin func()
	delfin func()
}

func (n *lruNode) rInsert(at *lruNode) {
	x := at.rNext
	at.rNext = n
	n.rPrev = at
	n.rNext = x
	x.rPrev = n
}

func (n *lruNode) rRemove() bool {
	// only remove if not already removed
	if n.rPrev == nil {
		return false
	}

	n.rPrev.rNext = n.rNext
	n.rNext.rPrev = n.rPrev
	n.rPrev = nil
	n.rNext = nil

	return true
}

func (n *lruNode) execFin() {
	if n.setfin != nil {
		n.setfin()
		n.setfin = nil
	}
	if n.delfin != nil {
		n.delfin()
		n.delfin = nil
	}
}

func (n *lruNode) doEvict() {
	if n.ns.zapped {
		return
	}

	// remove elem
	delete(n.ns.table, n.key)

	// execute finalizer
	n.execFin()
}

func (n *lruNode) evict() {
	n.ns.lru.Lock()
	n.evictNB()
	n.ns.lru.Unlock()
}

func (n *lruNode) evictNB() {
	n.ref--
	if n.ref == 0 {
		n.doEvict()
	} else if n.ref < 0 {
		panic("leveldb/cache: LRUCache: negative node reference")
	}
}

type lruObject struct {
	node *lruNode
	once uint32
}

func (p *lruObject) Value() interface{} {
	if atomic.LoadUint32(&p.once) == 0 {
		return p.node.value
	}
	return nil
}

func (p *lruObject) Release() {
	if !atomic.CompareAndSwapUint32(&p.once, 0, 1) {
		return
	}

	p.node.evict()
	p.node = nil
}
