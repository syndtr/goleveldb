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

// Package memdb provide in-memory key/value database implementation.
package memdb

import (
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const tMaxHeight = 12

type mNode struct {
	key   []byte
	value []byte
	next  []unsafe.Pointer
}

func newNode(key, value []byte, height int32) *mNode {
	return &mNode{key, value, make([]unsafe.Pointer, height)}
}

func (p *mNode) getNext(n int) *mNode {
	return (*mNode)(atomic.LoadPointer(&p.next[n]))
}

func (p *mNode) setNext(n int, x *mNode) {
	atomic.StorePointer(&p.next[n], unsafe.Pointer(x))
}

func (p *mNode) getNext_NB(n int) *mNode {
	return (*mNode)(p.next[n])
}

func (p *mNode) setNext_NB(n int, x *mNode) {
	p.next[n] = unsafe.Pointer(x)
}

// DB represent an in-memory key/value database.
type DB struct {
	cmp       comparer.BasicComparer
	rnd       *rand.Rand
	head      *mNode
	maxHeight int32
	kvSize    int64
	n         int32

	prev [tMaxHeight]*mNode
}

// New create new initalized in-memory key/value database.
func New(cmp comparer.BasicComparer) *DB {
	return &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		head:      newNode(nil, nil, tMaxHeight),
	}
}

// Put insert given key and value to the database. Need external synchronization.
// Key and value will not be copied; and should not modified after this point.
func (p *DB) Put(key []byte, value []byte) {
	if m, exact := p.findGE_NB(key, true); exact {
		h := int32(len(m.next))
		x := newNode(key, value, h)
		for i, n := range p.prev[:h] {
			x.setNext_NB(i, m.getNext_NB(i))
			n.setNext(i, x)
		}
		atomic.AddInt64(&p.kvSize, int64(len(value)-len(m.value)))
		return
	}

	h := p.randHeight()
	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			p.prev[i] = p.head
		}
		atomic.StoreInt32(&p.maxHeight, h)
	}

	x := newNode(key, value, h)
	for i, n := range p.prev[:h] {
		x.setNext_NB(i, n.getNext_NB(i))
		n.setNext(i, x)
	}

	atomic.AddInt64(&p.kvSize, int64(len(key)+len(value)))
	atomic.AddInt32(&p.n, 1)
}

// Remove remove given key from the database. Need external synchronization.
func (p *DB) Remove(key []byte) {
	x, exact := p.findGE_NB(key, true)
	if !exact {
		return
	}

	h := len(x.next)
	for i, n := range p.prev[:h] {
		n.setNext(i, n.getNext_NB(i).getNext_NB(i))
	}

	atomic.AddInt64(&p.kvSize, -int64(len(x.key)+len(x.value)))
	atomic.AddInt32(&p.n, -1)
}

// Contains return true if given key are in database.
func (p *DB) Contains(key []byte) bool {
	_, exact := p.findGE(key, false)
	return exact
}

// Get return value for the given key.
func (p *DB) Get(key []byte) (value []byte, err error) {
	if x, exact := p.findGE(key, false); exact {
		return x.value, nil
	}
	return nil, errors.ErrNotFound
}

// Find return key/value equal or greater than given key.
func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
	if x, _ := p.findGE(key, false); x != nil {
		return x.key, x.value, nil
	}
	return nil, nil, errors.ErrNotFound
}

// NewIterator create a new iterator over the database content.
func (p *DB) NewIterator() *Iterator {
	return &Iterator{p: p}
}

// Size return sum of key/value size.
func (p *DB) Size() int {
	return int(atomic.LoadInt64(&p.kvSize))
}

// Len return the number of entries in the database.
func (p *DB) Len() int {
	return int(atomic.LoadInt32(&p.n))
}

func (p *DB) findGE(key []byte, prev bool) (*mNode, bool) {
	x := p.head
	h := int(atomic.LoadInt32(&p.maxHeight)) - 1
	for {
		next := x.getNext(h)
		cmp := 1
		if next != nil {
			cmp = p.cmp.Compare(next.key, key)
		}
		if cmp < 0 {
			// Keep searching in this list
			x = next
		} else {
			if prev {
				p.prev[h] = x
			} else if cmp == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--
		}
	}
	return nil, false
}

func (p *DB) findGE_NB(key []byte, prev bool) (*mNode, bool) {
	x := p.head
	h := int(p.maxHeight) - 1
	for {
		next := x.getNext_NB(h)
		cmp := 1
		if next != nil {
			cmp = p.cmp.Compare(next.key, key)
		}
		if cmp < 0 {
			// Keep searching in this list
			x = next
		} else {
			if prev {
				p.prev[h] = x
			} else if cmp == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--
		}
	}
	return nil, false
}

func (p *DB) findLT(key []byte) *mNode {
	x := p.head
	h := int(atomic.LoadInt32(&p.maxHeight)) - 1
	for {
		next := x.getNext(h)
		if next == nil || p.cmp.Compare(next.key, key) >= 0 {
			if h == 0 {
				if x == p.head {
					return nil
				}
				return x
			}
			h--
		} else {
			x = next
		}
	}
	return nil
}

func (p *DB) findLast() *mNode {
	x := p.head
	h := int(atomic.LoadInt32(&p.maxHeight)) - 1
	for {
		next := x.getNext(h)
		if next == nil {
			if h == 0 {
				if x == p.head {
					return nil
				}
				return x
			}
			h--
		} else {
			x = next
		}
	}
	return nil
}

func (p *DB) randHeight() (h int32) {
	const branching = 4
	h = 1
	for h < tMaxHeight && p.rnd.Int()%branching == 0 {
		h++
	}
	return
}

type Iterator struct {
	p      *DB
	node   *mNode
	onLast bool
}

func (i *Iterator) Valid() bool {
	return i.node != nil
}

func (i *Iterator) First() bool {
	i.node = i.p.head.getNext(0)
	return i.Valid()
}

func (i *Iterator) Last() bool {
	i.node = i.p.findLast()
	return i.Valid()
}

func (i *Iterator) Seek(key []byte) (r bool) {
	i.node, _ = i.p.findGE(key, false)
	return i.Valid()
}

func (i *Iterator) Next() bool {
	if i.node == nil {
		return i.First()
	}
	i.node = i.node.getNext(0)
	res := i.Valid()
	if !res {
		i.onLast = true
	}
	return res
}

func (i *Iterator) Prev() bool {
	if i.node == nil {
		if i.onLast {
			return i.Last()
		}
		return false
	}
	i.node = i.p.findLT(i.node.key)
	return i.Valid()
}

func (i *Iterator) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.node.key
}

func (i *Iterator) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.node.value
}

func (i *Iterator) Error() error { return nil }
