// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator

import "github.com/syndtr/goleveldb/leveldb/comparer"

// MergedIterator represent a merged iterators. MergedIterator can be used
// to merge multiple iterators into one.
type MergedIterator struct {
	cmp   comparer.Comparer
	iters []Iterator

	iter     Iterator
	backward bool
	last     bool
	err      error
}

// NewMergedIterator create new initialized merged iterators.
func NewMergedIterator(iters []Iterator, cmp comparer.Comparer) *MergedIterator {
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

	if !i.iter.Next() && i.iter.Error() != nil {
		i.err = i.iter.Error()
		return false
	}
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

	if !i.iter.Prev() && i.iter.Error() != nil {
		i.err = i.iter.Error()
		return false
	}
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
