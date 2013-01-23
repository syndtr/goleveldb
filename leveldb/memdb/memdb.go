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

var (
	mPtrSize  int
	mNodeSize int
)

func init() {
	node := new(mNode)
	mPtrSize = int(unsafe.Sizeof(node))
	mNodeSize = int(unsafe.Sizeof(*node))
}

type mNode struct {
	key   []byte
	value []byte
	next  []unsafe.Pointer
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
	maxHeight int
	memSize   int64
	n         int32

	prev [tMaxHeight]*mNode
}

// New create new initalized in-memory key/value database.
func New(cmp comparer.BasicComparer) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
	}
	p.head = p.newNode(nil, nil, tMaxHeight)
	return p
}

// Put insert given key and value to the database. Need external synchronization.
func (p *DB) Put(key []byte, value []byte) {
	p.findGE_NB(key, true)

	h := p.randHeight()
	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			p.prev[i] = p.head
		}
		p.maxHeight = h
	}

	x := p.newNode(key, value, h)
	for i, n := range p.prev[:h] {
		x.setNext_NB(i, n.getNext_NB(i))
		n.setNext(i, x)
	}

	atomic.AddInt64(&p.memSize, int64(mNodeSize+(mPtrSize*h)+len(key)+len(value)))
	atomic.AddInt32(&p.n, 1)
}

// Remove remove given key from the database. Need external synchronization.
func (p *DB) Remove(key []byte) {
	x := p.findGE_NB(key, true)
	if x == nil || x == p.head || p.cmp.Compare(x.key, key) != 0 {
		return
	}

	h := len(x.next)
	for i, n := range p.prev[:h] {
		n.setNext(i, n.getNext_NB(i).getNext_NB(i))
	}

	atomic.AddInt64(&p.memSize, -int64(mNodeSize+(mPtrSize*h)+len(x.key)+len(x.value)))
	atomic.AddInt32(&p.n, -1)
}

// Contains return true if given key are in database.
func (p *DB) Contains(key []byte) bool {
	x := p.findGE(key, false)
	if x != nil && x != p.head && p.cmp.Compare(x.key, key) == 0 {
		return true
	}
	return false
}

// Get return key/value equal or greater than given key.
func (p *DB) Get(key []byte) (rkey, value []byte, err error) {
	node := p.findGE(key, false)
	if node == nil || node == p.head {
		err = errors.ErrNotFound
		return
	}
	return node.key, node.value, nil
}

// NewIterator create a new iterator over the database content.
func (p *DB) NewIterator() *Iterator {
	return &Iterator{p: p}
}

// Size return approximate size of memory used by the database.
func (p *DB) Size() int {
	return int(atomic.LoadInt64(&p.memSize))
}

// Len return the number of entries in the database.
func (p *DB) Len() int {
	return int(atomic.LoadInt32(&p.n))
}

func (p *DB) newNode(key, value []byte, height int) *mNode {
	return &mNode{key, value, make([]unsafe.Pointer, height)}
}

func (p *DB) findGE(key []byte, prev bool) *mNode {
	x := p.head
	n := p.maxHeight - 1
	for {
		next := x.getNext(n)
		if next != nil && p.cmp.Compare(next.key, key) < 0 {
			// Keep searching in this list
			x = next
		} else {
			if prev {
				p.prev[n] = x
			}
			if n == 0 {
				return next
			}
			n--
		}
	}
	return nil
}

func (p *DB) findGE_NB(key []byte, prev bool) *mNode {
	x := p.head
	n := p.maxHeight - 1
	for {
		next := x.getNext_NB(n)
		if next != nil && p.cmp.Compare(next.key, key) < 0 {
			// Keep searching in this list
			x = next
		} else {
			if prev {
				p.prev[n] = x
			}
			if n == 0 {
				return next
			}
			n--
		}
	}
	return nil
}

func (p *DB) findLT(key []byte) *mNode {
	x := p.head
	n := p.maxHeight - 1
	for {
		next := x.getNext(n)
		if next == nil || p.cmp.Compare(next.key, key) >= 0 {
			if n == 0 {
				return x
			}
			n--
		} else {
			x = next
		}
	}
	return nil
}

func (p *DB) findLast() *mNode {
	x := p.head
	n := p.maxHeight - 1
	for {
		next := x.getNext(n)
		if next == nil {
			if n == 0 {
				return x
			}
			n--
		} else {
			x = next
		}
	}
	return nil
}

func (p *DB) randHeight() int {
	const branching = 4
	n := 1
	for n < tMaxHeight && p.rnd.Int()%branching == 0 {
		n++
	}
	return n
}

type Iterator struct {
	p      *DB
	node   *mNode
	onLast bool
}

func (i *Iterator) Valid() bool {
	return i.node != nil && i.node != i.p.head
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
	i.node = i.p.findGE(key, false)
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
	if i.node == i.p.head {
		i.node = nil
	}
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
