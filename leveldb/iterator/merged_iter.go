// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator

import (
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type dir int

const (
	dirReleased dir = iota - 1
	dirSOI
	dirEOI
	dirBackward
	dirForward
)

type mergedIterator struct {
	cmp    comparer.Comparer
	iters  []Iterator
	strict bool

	keys     [][]byte
	index    int
	dir      dir
	err      error
	errf     func(err error)
	releaser util.Releaser

	heap indexHeap
}

func assertKey(key []byte) []byte {
	if key == nil {
		panic("leveldb/iterator: nil key")
	}
	return key
}

func (i *mergedIterator) iterErr(iter Iterator) bool {
	if err := iter.Error(); err != nil {
		if i.errf != nil {
			i.errf(err)
		}
		if i.strict || !errors.IsCorrupted(err) {
			i.err = err
			return true
		}
	}
	return false
}

func (i *mergedIterator) Valid() bool {
	return i.err == nil && i.dir > dirEOI
}

func (i *mergedIterator) First() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	i.heap.Reset()
	for x, iter := range i.iters {
		switch {
		case iter.First():
			i.keys[x] = assertKey(iter.Key())
			i.heap.Add(x)
		case i.iterErr(iter):
			return false
		default:
			i.keys[x] = nil
		}
	}
	i.heap.Init(false)
	i.dir = dirSOI
	return i.next()
}

func (i *mergedIterator) Last() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	i.heap.Reset()
	for x, iter := range i.iters {
		switch {
		case iter.Last():
			i.keys[x] = assertKey(iter.Key())
			i.heap.Add(x)
		case i.iterErr(iter):
			return false
		default:
			i.keys[x] = nil
		}
	}
	i.heap.Init(true)
	i.dir = dirEOI
	return i.prev()
}

func (i *mergedIterator) Seek(key []byte) bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	i.heap.Reset()
	for x, iter := range i.iters {
		switch {
		case iter.Seek(key):
			i.keys[x] = assertKey(iter.Key())
			i.heap.Add(x)
		case i.iterErr(iter):
			return false
		default:
			i.keys[x] = nil
		}
	}
	i.heap.Init(false)
	i.dir = dirSOI
	return i.next()
}

func (i *mergedIterator) next() bool {
	if i.heap.Empty() {
		i.dir = dirEOI
		return false
	}
	i.index = i.heap.Top()
	i.dir = dirForward
	return true
}

func (i *mergedIterator) Next() bool {
	if i.dir == dirEOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	switch i.dir {
	case dirSOI:
		return i.First()
	case dirBackward:
		key := append([]byte(nil), i.keys[i.index]...)
		if !i.Seek(key) {
			return false
		}
		return i.Next()
	}

	x := i.index
	iter := i.iters[x]
	switch {
	case iter.Next():
		i.keys[x] = assertKey(iter.Key())
		i.heap.FixTopWith(x)
	case i.iterErr(iter):
		return false
	default:
		i.keys[x] = nil
		i.heap.Pop()
	}
	return i.next()
}

func (i *mergedIterator) prev() bool {
	if i.heap.Empty() {
		i.dir = dirSOI
		return false
	}
	i.index = i.heap.Top()
	i.dir = dirBackward
	return true
}

func (i *mergedIterator) Prev() bool {
	if i.dir == dirSOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	switch i.dir {
	case dirEOI:
		return i.Last()
	case dirForward:
		i.heap.Reset()
		for x, iter := range i.iters {
			if x == i.index {
				i.heap.Add(x)
				continue
			}
			seek := iter.Seek(i.keys[i.index])
			switch {
			case seek && iter.Prev(), !seek && iter.Last():
				i.keys[x] = assertKey(iter.Key())
				i.heap.Add(x)
			case i.iterErr(iter):
				return false
			default:
				i.keys[x] = nil
			}
		}
		i.heap.Init(true)
	}

	x := i.index
	iter := i.iters[x]
	switch {
	case iter.Prev():
		i.keys[x] = assertKey(iter.Key())
		i.heap.FixTopWith(x)
	case i.iterErr(iter):
		return false
	default:
		i.keys[x] = nil
		i.heap.Pop()
	}
	return i.prev()
}

func (i *mergedIterator) Key() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.keys[i.index]
}

func (i *mergedIterator) Value() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.iters[i.index].Value()
}

func (i *mergedIterator) Release() {
	if i.dir != dirReleased {
		i.dir = dirReleased
		for _, iter := range i.iters {
			iter.Release()
		}
		i.iters = nil
		i.keys = nil
		i.heap.indexes = nil
		i.heap.keys = nil
		if i.releaser != nil {
			i.releaser.Release()
			i.releaser = nil
		}
	}
}

func (i *mergedIterator) SetReleaser(releaser util.Releaser) {
	if i.dir == dirReleased {
		panic(util.ErrReleased)
	}
	if i.releaser != nil && releaser != nil {
		panic(util.ErrHasReleaser)
	}
	i.releaser = releaser
}

func (i *mergedIterator) Error() error {
	return i.err
}

func (i *mergedIterator) SetErrorCallback(f func(err error)) {
	i.errf = f
}

// NewMergedIterator returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp.
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
// None of the iters may be nil.
//
// If strict is true the any 'corruption errors' (i.e errors.IsCorrupted(err) == true)
// won't be ignored and will halt 'merged iterator', otherwise the iterator will
// continue to the next 'input iterator'.
func NewMergedIterator(iters []Iterator, cmp comparer.Comparer, strict bool) Iterator {
	keys := make([][]byte, len(iters))
	return &mergedIterator{
		iters:  iters,
		cmp:    cmp,
		strict: strict,
		keys:   keys,
		heap: indexHeap{
			indexes: make([]int, 0, len(iters)),
			keys:    keys,
			cmp:     cmp,
		},
	}
}

// indexHeap provides heap operations for indexes.
// It specializes 'heap' with int element type.
type indexHeap struct {
	indexes []int
	keys    [][]byte
	cmp     comparer.Comparer
	reverse bool
}

func (h *indexHeap) Init(reverse bool) {
	h.reverse = reverse
	// heapify
	n := len(h.indexes)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *indexHeap) Reset() {
	h.indexes = h.indexes[:0]
}

func (h *indexHeap) Add(x int) {
	h.indexes = append(h.indexes, x)
}

func (h *indexHeap) Empty() bool { return len(h.indexes) == 0 }
func (h *indexHeap) Top() int    { return h.indexes[0] }

func (h *indexHeap) Pop() int {
	top := h.indexes[0]
	n := len(h.indexes) - 1
	h.swap(0, n)
	h.down(0, n)
	h.indexes = h.indexes[:n]
	return top
}

func (h *indexHeap) FixTopWith(x int) {
	// replace top
	h.indexes[0] = x
	// and then fix
	h.down(0, len(h.indexes))
}

func (h *indexHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
	return i > i0
}

func (h *indexHeap) less(i, j int) bool {
	i, j = h.indexes[i], h.indexes[j]
	r := h.cmp.Compare(h.keys[i], h.keys[j])
	if h.reverse {
		return r > 0
	}
	return r < 0
}

func (h *indexHeap) swap(i, j int) {
	h.indexes[i], h.indexes[j] = h.indexes[j], h.indexes[i]
}
