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

package leveldb

type IteratorSeeker interface {
	// An iterator is either positioned at a key/value pair, or
	// not valid.  This method returns true if the iterator is valid.
	Valid() bool

	// Position at the first key in the source.  The iterator is Valid()
	// after this call if the source is not empty.
	First() bool

	// Position at the last key in the source.  The iterator is
	// Valid() after this call if the source is not empty.
	Last() bool

	// Position at the first key in the source that at or past given 'key'
	// The iterator is Valid() after this call if the source contains
	// an entry that comes at or past given 'key'.
	Seek(key []byte) bool

	// Moves to the next entry in the source.  After this call, Valid() is
	// true if the iterator was not positioned at the last entry in the source.
	// REQUIRES: Valid()
	Next() bool

	// Moves to the previous entry in the source.  After this call, Valid() is
	// true if the iterator was not positioned at the first entry in source.
	// REQUIRES: Valid()
	Prev() bool

	// If an error has occurred, return it.  Else return nil.
	Error() error
}

type Iterator interface {
	IteratorSeeker

	// Return the key for the current entry.  The underlying storage for
	// the returned slice is valid only until the next modification of
	// the iterator.
	// REQUIRES: Valid()
	Key() []byte

	// Return the value for the current entry.  The underlying storage for
	// the returned slice is valid only until the next modification of
	// the iterator.
	// REQUIRES: !AtEnd() && !AtStart()
	Value() []byte
}

type IteratorIndexer interface {
	IteratorSeeker

	// Return iterator for current entry.
	Get() (Iterator, error)
}

type EmptyIterator struct {
	Err error
}

func (*EmptyIterator) Valid() bool          { return false }
func (*EmptyIterator) First() bool          { return false }
func (*EmptyIterator) Last() bool           { return false }
func (*EmptyIterator) Seek(key []byte) bool { return false }
func (*EmptyIterator) Next() bool           { return false }
func (*EmptyIterator) Prev() bool           { return false }
func (*EmptyIterator) Key() []byte          { return nil }
func (*EmptyIterator) Value() []byte        { return nil }
func (i *EmptyIterator) Error() error       { return i.Err }

type IndexedIterator struct {
	index IteratorIndexer
	data  Iterator
	err   error
}

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
func (i *IndexedIterator) Error() error {
	if i.err != nil {
		return i.err
	} else if i.index.Error() != nil {
		return i.index.Error()
	} else if i.data != nil && i.data.Error() != nil {
		return i.data.Error()
	}
	return nil
}

func (i *IndexedIterator) setData() bool {
	i.data, i.err = i.index.Get()
	return i.err == nil
}

type MergedIterator struct {
	cmp   Comparator
	iters []Iterator

	iter     Iterator
	backward bool
	last     bool
	err      error
}

func NewMergedIterator(iters []Iterator, cmp Comparator) *MergedIterator {
	return &MergedIterator{iters: iters, cmp: cmp}
}

func (i *MergedIterator) Valid() bool {
	return i.err == nil && i.iter != nil
}

func (i *MergedIterator) First() bool {
	if i.err != nil {
		return false
	}

	for _, p := range i.iters {
		if !p.First() && p.Error() != nil {
			i.err = p.Error()
			return false
		}
	}
	i.smallest()
	i.backward = false
	i.last = false
	return i.iter != nil
}

func (i *MergedIterator) Last() bool {
	if i.err != nil {
		return false
	}

	for _, p := range i.iters {
		if !p.Last() && p.Error() != nil {
			i.err = p.Error()
			return false
		}
	}
	i.largest()
	i.backward = true
	i.last = false
	return i.iter != nil
}

func (i *MergedIterator) Seek(key []byte) bool {
	if i.err != nil {
		return false
	}

	for _, p := range i.iters {
		if !p.Seek(key) && p.Error() != nil {
			i.err = p.Error()
			return false
		}
	}
	i.smallest()
	i.backward = false
	i.last = i.iter == nil
	return !i.last
}

func (i *MergedIterator) Next() bool {
	if i.err != nil {
		return false
	}

	if i.iter == nil {
		if !i.last {
			return i.First()
		}
		return false
	}

	if i.backward {
		key := i.iter.Key()
		for _, p := range i.iters {
			if p == i.iter {
				continue
			}
			if p.Seek(key) && i.cmp.Compare(key, p.Key()) == 0 {
				p.Next()
			}
			if p.Error() != nil {
				i.err = p.Error()
				return false
			}
		}
		i.backward = false
	}

	i.iter.Next()
	i.smallest()
	i.last = i.iter == nil
	return !i.last
}

func (i *MergedIterator) Prev() bool {
	if i.err != nil {
		return false
	}

	if i.iter == nil {
		if i.last {
			return i.Last()
		}
		return false
	}

	if !i.backward {
		key := i.iter.Key()
		for _, p := range i.iters {
			if p == i.iter {
				continue
			}
			if p.Seek(key) {
				p.Prev()
			} else {
				p.Last()
			}
			if p.Error() != nil {
				i.err = p.Error()
				return false
			}
		}
		i.backward = true
	}

	i.iter.Prev()
	i.largest()
	return i.iter != nil
}

func (i *MergedIterator) Key() []byte {
	if i.iter == nil || i.err != nil {
		return nil
	}
	return i.iter.Key()
}

func (i *MergedIterator) Value() []byte {
	if i.iter == nil || i.err != nil {
		return nil
	}
	return i.iter.Value()
}

func (i *MergedIterator) Error() error {
	return i.err
}

func (i *MergedIterator) smallest() {
	i.iter = nil
	for _, p := range i.iters {
		if !p.Valid() {
			continue
		}
		if i.iter == nil || i.cmp.Compare(p.Key(), i.iter.Key()) < 0 {
			i.iter = p
		}
	}
}

func (i *MergedIterator) largest() {
	i.iter = nil
	for _, p := range i.iters {
		if !p.Valid() {
			continue
		}
		if i.iter == nil || i.cmp.Compare(p.Key(), i.iter.Key()) > 0 {
			i.iter = p
		}
	}
}
