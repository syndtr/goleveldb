// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator

import (
	"sort"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Array is the interface that wraps basic Index and Len method.
type Array interface {
	// Index returns key/value pair with index of i.
	Index(i int) (key, value []byte)

	// Len returns length of the array.
	Len() int
}

// Array is the interface that wraps Array and basic Get method.
type ArrayIndexer interface {
	Array

	// Get returns a new data iterator with index of i.
	Get(i int) Iterator
}

type arrayIterator struct {
	util.BasicReleaser
	array      Array
	indexer    ArrayIndexer
	cmp        comparer.Comparer
	pos        int
	key, value []byte
}

func (i *arrayIterator) setKV() {
	i.key, i.value = i.array.Index(i.pos)
}

func (i *arrayIterator) clearKV() {
	i.key = nil
	i.value = nil
}

func (i *arrayIterator) Valid() bool {
	if i.pos < 0 || i.pos >= i.array.Len() {
		return false
	}
	return true
}

func (i *arrayIterator) First() bool {
	if i.array.Len() == 0 {
		i.pos = -1
		i.clearKV()
		return false
	}
	i.pos = 0
	i.setKV()
	return true
}

func (i *arrayIterator) Last() bool {
	n := i.array.Len()
	if n == 0 {
		i.pos = 0
		return false
	}
	i.pos = n - 1
	i.setKV()
	return true
}

func (i *arrayIterator) Seek(key []byte) bool {
	n := i.array.Len()
	if n == 0 {
		i.pos = 0
		return false
	}
	i.pos = sort.Search(n, func(x int) bool {
		key_, _ := i.array.Index(x)
		return i.cmp.Compare(key_, key) >= 0
	})
	if i.pos >= n {
		i.clearKV()
		return false
	}
	i.setKV()
	return true
}

func (i *arrayIterator) Next() bool {
	i.pos++
	if n := i.array.Len(); i.pos >= n {
		i.pos = n
		i.clearKV()
		return false
	}
	i.setKV()
	return true
}

func (i *arrayIterator) Prev() bool {
	i.pos--
	if i.pos < 0 {
		i.pos = -1
		i.clearKV()
		return false
	}
	i.setKV()
	return true
}

func (i *arrayIterator) Key() []byte {
	return i.key
}

func (i *arrayIterator) Value() []byte {
	return i.value
}

func (i *arrayIterator) Get() Iterator {
	if i.Valid() && i.indexer != nil {
		return i.indexer.Get(i.pos)
	}
	return nil
}

func (i *arrayIterator) Error() error { return nil }

// NewArrayIterator returns an iterator from the given array. The given
// array should in strictly increasing key order, as defined by cmp.
func NewArrayIterator(array Array, cmp comparer.Comparer) Iterator {
	return &arrayIterator{array: array, cmp: cmp, pos: -1}
}

// NewArrayIndexer returns an index iterator from the given array. The
// given array should in strictly increasing key order, as defined by cmp.
func NewArrayIndexer(array ArrayIndexer, cmp comparer.Comparer) IteratorIndexer {
	return &arrayIterator{array: array, indexer: array, cmp: cmp, pos: -1}
}
