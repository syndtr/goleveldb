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
		n.deleted = true
		n.rRemove()
		n.delfin = fin
		n.evict_NB()
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
		n.rRemove()
		n.evict_NB()
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

func (p *lruNs) Get(key uint64, setf SetFunc) (obj Object, ok bool) {
	lru := p.lru
	lru.Lock()

	if p.zapped {
		lru.Unlock()
		if setf == nil {
			return
		}
		if ok, value, _, fin := setf(); ok {
			return &emptyCacheObj{value, fin}, true
		}
		return
	}

	n, ok := p.table[key]
	if ok {
		if !n.deleted {
			// bump to front
			n.rRemove()
			n.rInsert(&lru.recent)
		}
		atomic.AddInt32(&n.ref, 1)
	} else {
		if setf == nil {
			lru.Unlock()
			return
		}

		ok, value, charge, fin := setf()
		if !ok {
			lru.Unlock()
			return nil, false
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

	return &lruObject{node: n}, true
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

	if n.deleted {
		lru.Unlock()
		return false
	}

	n.deleted = true
	n.rRemove()
	n.delfin = fin
	n.evict_NB()
	lru.size -= n.charge

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
		if n.deleted {
			continue
		}
		n.rRemove()
		n.delfin = fin
		n.evict_NB()
		lru.size -= n.charge
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
		if n.rRemove() {
			lru.size -= n.charge
		}
		n.execFin()
	}
	p.zapped = true
	p.table = nil
	delete(lru.table, p.id)
	lru.Unlock()
}

type lruNode struct {
	ns *lruNs

	rNext, rPrev *lruNode

	key     uint64
	value   interface{}
	charge  int
	ref     int32
	deleted bool
	setfin  func()
	delfin  func()
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

	n.value = nil
}

func (n *lruNode) evict() {
	if atomic.AddInt32(&n.ref, -1) != 0 {
		return
	}

	lru := n.ns.lru
	lru.Lock()
	n.doEvict()
	lru.Unlock()
}

func (n *lruNode) evict_NB() {
	if atomic.AddInt32(&n.ref, -1) != 0 {
		return
	}

	n.doEvict()
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
