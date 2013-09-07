// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator

// IteratorIndexer is the interface that group IteratorSeeker and basic Get
// method. An index of indexed iterator need to implement this interface.
type IteratorIndexer interface {
	IteratorSeeker

	// Return iterator for current entry.
	Get() (Iterator, error)
}

// IndexedIterator represent an indexed interator. IndexedIterator can be used
// to access an indexed data, which the index is a pointer to actual data.
type IndexedIterator struct {
	index IteratorIndexer
	data  Iterator
	err   error
}

// NewIndexedIterator create new initialized indexed iterator.
func NewIndexedIterator(index IteratorIndexer) *IndexedIterator {
	return &IndexedIterator{index: index}
}

func (i *IndexedIterator) Valid() bool {
	return i.data != nil && i.data.Valid()
}

func (i *IndexedIterator) First() bool {
	if i.err != nil {
		return false
	}

	if !i.index.First() || !i.setData() {
		i.data = nil
		return false
	}
	return i.Next()
}

func (i *IndexedIterator) Last() bool {
	if i.err != nil {
		return false
	}

	if !i.index.Last() || !i.setData() {
		i.data = nil
		return false
	}
	if !i.data.Last() {
		// empty data block, try prev block
		i.data = nil
		return i.Prev()
	}
	return true
}

func (i *IndexedIterator) Seek(key []byte) bool {
	if i.err != nil {
		return false
	}

	if !i.index.Seek(key) || !i.setData() {
		i.data = nil
		return false
	}
	if !i.data.Seek(key) {
		return i.Next()
	}
	return true
}

func (i *IndexedIterator) Next() bool {
	if i.err != nil {
		return false
	}

	if i.data == nil || !i.data.Next() {
		if !i.index.Next() || !i.setData() {
			i.data = nil
			return false
		}
		return i.Next()
	}
	return true
}

func (i *IndexedIterator) Prev() bool {
	if i.err != nil {
		return false
	}

	if i.data == nil || !i.data.Prev() {
		if !i.index.Prev() || !i.setData() {
			i.data = nil
			return false
		}
		if !i.data.Last() {
			// empty data block, try prev block
			i.data = nil
			return i.Prev()
		}
		return true
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
	if i.data != nil {
		i.data.Release()
		i.data = nil
	}
	i.index.Release()
}

func (i *IndexedIterator) Error() (err error) {
	if i.err != nil {
		err = i.err
	} else if i.index.Error() != nil {
		err = i.index.Error()
	} else if i.data != nil && i.data.Error() != nil {
		err = i.data.Error()
	}
	return
}

func (i *IndexedIterator) setData() bool {
	i.data, i.err = i.index.Get()
	return i.err == nil
}
