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
	"leveldb/comparer"
	"leveldb/errors"
	"math/rand"
	"sync"
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
	next  []*mNode
}

// DB represent an in-memory key/value database.
type DB struct {
	cmp       comparer.BasicComparer
	rnd       *rand.Rand
	head      *mNode
	maxHeight int
	memSize   int
	n         int

	mu   sync.RWMutex
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

// Put insert given key and value to the database.
func (p *DB) Put(key []byte, value []byte) {
	p.mu.Lock()
	p.findGreaterOrEqual(key, true)

	h := p.randHeight()
	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			p.prev[i] = p.head
		}
		p.maxHeight = h
	}

	x := p.newNode(key, value, h)
	for i, n := range p.prev[:h] {
		x.next[i] = n.next[i]
		n.next[i] = x
	}

	p.memSize += mNodeSize + (mPtrSize * h)
	p.memSize += len(key) + len(value)
	p.n++
	p.mu.Unlock()
}

// Remove remove given key from the database.
func (p *DB) Remove(key []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	x := p.findGreaterOrEqual(key, true)
	if x == nil || x == p.head || p.cmp.Compare(x.key, key) != 0 {
		return
	}

	h := len(x.next)
	for i, n := range p.prev[:h] {
		n.next[i] = n.next[i].next[i]
	}

	p.memSize -= mNodeSize + (mPtrSize * h)
	p.memSize -= len(x.key) + len(x.value)
	p.n--
}

// Contains return true if given key are in database.
func (p *DB) Contains(key []byte) bool {
	p.mu.RLock()
	x := p.findGreaterOrEqual(key, false)
	p.mu.RUnlock()
	if x != nil && x != p.head && p.cmp.Compare(x.key, key) == 0 {
		return true
	}
	return false
}

// Get return key/value equal or greater than given key.
func (p *DB) Get(key []byte) (rkey, value []byte, err error) {
	p.mu.RLock()
	node := p.findGreaterOrEqual(key, false)
	p.mu.RUnlock()
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
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.memSize
}

// Len return the number of entries in the database.
func (p *DB) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.n
}

func (p *DB) newNode(key, value []byte, height int) *mNode {
	return &mNode{key, value, make([]*mNode, height)}
}

func (p *DB) findGreaterOrEqual(key []byte, prev bool) *mNode {
	x := p.head
	n := p.maxHeight - 1
	for {
		next := x.next[n]
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

func (p *DB) findLessThan(key []byte) *mNode {
	x := p.head
	n := p.maxHeight - 1
	for {
		next := x.next[n]
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
		next := x.next[n]
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
	i.p.mu.RLock()
	i.node = i.p.head.next[0]
	i.p.mu.RUnlock()
	return i.Valid()
}

func (i *Iterator) Last() bool {
	i.p.mu.RLock()
	i.node = i.p.findLast()
	i.p.mu.RUnlock()
	return i.Valid()
}

func (i *Iterator) Seek(key []byte) (r bool) {
	i.p.mu.RLock()
	i.node = i.p.findGreaterOrEqual(key, false)
	i.p.mu.RUnlock()
	return i.Valid()
}

func (i *Iterator) Next() bool {
	if i.node == nil {
		return i.First()
	}
	i.p.mu.RLock()
	i.node = i.node.next[0]
	i.p.mu.RUnlock()
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
	i.p.mu.RLock()
	i.node = i.p.findLessThan(i.node.key)
	i.p.mu.RUnlock()
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
