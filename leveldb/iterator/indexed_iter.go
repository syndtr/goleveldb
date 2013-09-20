// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator

import (
	"github.com/syndtr/goleveldb/leveldb/util"
)

// IteratorIndexer is the interface that wraps IteratorSeeker and basic Get
// method. IteratorIndexer provides index for indexed iterator.
type IteratorIndexer interface {
	IteratorSeeker

	// Get returns a new data iterator for the current position, or nil if
	// done.
	Get() Iterator
}

type IndexedIterator struct {
	util.BasicReleaser
	index  IteratorIndexer
	strict bool

	data Iterator
	err  error
}

// NewIndexedIterator returns an indexed iterator. An index is iterator
// that returns another iterator, a data iterator. A data iterator is the
// iterator that contains actual key/value pairs.
//
// If strict is true then error yield by data iterator will halt the indexed
// iterator, on contrary if strict is false then the indexed iterator will
// ignore those error and move on to the next index.
func NewIndexedIterator(index IteratorIndexer, strict bool) *IndexedIterator {
	return &IndexedIterator{index: index, strict: strict}
}

func (i *IndexedIterator) setData() {
	if i.data != nil {
		i.data.Release()
	}
	i.data = i.index.Get()
}

func (i *IndexedIterator) clearData() {
	if i.data != nil {
		i.data.Release()
	}
	i.data = nil
}

func (i *IndexedIterator) dataErr() bool {
	if i.strict {
		if err := i.data.Error(); err != nil {
			i.err = err
			return true
		}
	}
	return false
}

func (i *IndexedIterator) Valid() bool {
	return i.data != nil && i.data.Valid()
}

func (i *IndexedIterator) First() bool {
	if i.err != nil {
		return false
	}

	if !i.index.First() {
		i.clearData()
		return false
	}
	i.setData()
	return i.Next()
}

func (i *IndexedIterator) Last() bool {
	if i.err != nil {
		return false
	}

	if !i.index.Last() {
		i.clearData()
		return false
	}
	i.setData()
	if !i.data.Last() {
		if i.dataErr() {
			return false
		}
		i.clearData()
		return i.Prev()
	}
	return true
}

func (i *IndexedIterator) Seek(key []byte) bool {
	if i.err != nil {
		return false
	}

	if !i.index.Seek(key) {
		i.clearData()
		return false
	}
	i.setData()
	if !i.data.Seek(key) {
		if i.dataErr() {
			return false
		}
		i.clearData()
		return i.Next()
	}
	return true
}

func (i *IndexedIterator) Next() bool {
	if i.err != nil {
		return false
	}

	switch {
	case i.data != nil && !i.data.Next():
		if i.dataErr() {
			return false
		}
		i.clearData()
		fallthrough
	case i.data == nil:
		if !i.index.Next() {
			return false
		}
		i.setData()
		return i.Next()
	}
	return true
}

func (i *IndexedIterator) Prev() bool {
	if i.err != nil {
		return false
	}

	switch {
	case i.data != nil && !i.data.Prev():
		if i.dataErr() {
			return false
		}
		i.clearData()
		fallthrough
	case i.data == nil:
		if !i.index.Prev() {
			return false
		}
		i.setData()
		if !i.data.Last() {
			if i.dataErr() {
				return false
			}
			i.clearData()
			return i.Prev()
		}
	}
	return true
}

func (i *IndexedIterator) Key() []byte {
	if i.data == nil {
		return nil
	}
	return i.data.Key()
}

func (i *IndexedIterator) Value() []byte {
	if i.data == nil {
		return nil
	}
	return i.data.Value()
}

func (i *IndexedIterator) Release() {
	i.clearData()
	i.index.Release()
	i.BasicReleaser.Release()
}

func (i *IndexedIterator) Error() error {
	if i.err != nil {
		return i.err
	}
	if err := i.index.Error(); err != nil {
		return err
	}
	return nil
}
