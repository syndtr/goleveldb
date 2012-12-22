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

package memdb

import (
	"bytes"
	"leveldb"
	"math/rand"
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

func (p *mNode) Next(n int) *mNode {
	return p.next[n]
}

func (p *mNode) SetNext(n int, x *mNode) {
	p.next[n] = x
}

type DB struct {
	cmp       leveldb.BasicComparator
	rnd       *rand.Rand
	head      *mNode
	maxHeight int
	memSize   int
	n         int
}

func New(cmp leveldb.BasicComparator) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
	}
	p.head = p.newNode(nil, nil, tMaxHeight)
	return p
}

func (p *DB) Put(key []byte, value []byte) {
	prev := make([]*mNode, tMaxHeight)
	x := p.findGreaterOrEqual(key, prev)
	n := p.randHeight()
	if n > p.maxHeight {
		for i := p.maxHeight; i < n; i++ {
			prev[i] = p.head
		}
		p.maxHeight = n
	}

	x = p.newNode(key, value, n)
	for i := 0; i < n; i++ {
		x.SetNext(i, prev[i].Next(i))
		prev[i].SetNext(i, x)
	}

	p.n++
}

func (p *DB) Contains(key []byte) bool {
	x := p.findGreaterOrEqual(key, nil)
	if x != nil && bytes.Equal(x.key, key) {
		return true
	}
	return false
}

func (p *DB) Get(key []byte) (rkey, value []byte, err error) {
	i := p.NewIterator()
	if !i.Seek(key) {
		return nil, nil, leveldb.ErrNotFound
	}
	return i.Key(), i.Value(), nil
}

func (p *DB) NewIterator() *Iterator {
	return &Iterator{p: p}
}

func (p *DB) Size() int {
	return p.memSize
}

func (p *DB) Len() int {
	return p.n
}

func (p *DB) newNode(key, value []byte, height int) *mNode {
	p.memSize += mNodeSize + (mPtrSize * height)
	p.memSize += len(key) + len(value)
	return &mNode{key, value, make([]*mNode, height)}
}

func (p *DB) findGreaterOrEqual(key []byte, prev []*mNode) *mNode {
	x := p.head
	n := p.maxHeight - 1
	for {
		next := x.Next(n)
		if next != nil && p.cmp.Compare(next.key, key) < 0 {
			// Keep searching in this list
			x = next
		} else {
			if prev != nil {
				prev[n] = x
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
		next := x.Next(n)
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
		next := x.Next(n)
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
	i.node = i.p.head.Next(0)
	return i.Valid()
}

func (i *Iterator) Last() bool {
	i.node = i.p.findLast()
	return i.Valid()
}

func (i *Iterator) Seek(key []byte) (r bool) {
	i.node = i.p.findGreaterOrEqual(key, nil)
	return i.Valid()
}

func (i *Iterator) Next() bool {
	if i.node == nil {
		return i.First()
	}
	i.node = i.node.Next(0)
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
	i.node = i.p.findLessThan(i.node.key)
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
