// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package iterator provides interface and implementation to traverse over
// contents of a database.
package iterator

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb/util"
)

// Iterator iterates over a DB's key/value pairs in key order.
//
// When encouter an error any 'seeks method' will return false and will
// yield no key/value pairs. The error can be queried by calling
// IteratorSeeker.Error method. Calling Release is still necessary.
//
// An iterator must be released after use, but it is not necessary to read
// an iterator until exhaustion.
//
// An iterator is not necessarily goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
type Iterator interface {
	IteratorSeeker

	// Key returns the key of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to any method of IteratorSeeker.
	Key() []byte

	// Value returns the key of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to any method of IteratorSeeker.
	Value() []byte
}

// IteratorSeeker is the interface that wraps 'iterator seeker methods'.
// Iterator seeker provides functionality to manipulate an iterator and
// query its status.
type IteratorSeeker interface {
	// util.Releaser is the interface that wraps basic Release method.
	// When called Release will releases any resources associated with the
	// iterator.
	util.Releaser

	// util.ReleaseSetter is the interface that wraps the basic SetReleaser
	// method.
	util.ReleaseSetter

	// TODO: Remove this when ready.
	Valid() bool

	// First moves the iterator to the first key/value pair. If the iterator
	// only contains one key/value pair then First and Last whould moves
	// to the same key/value pair.
	// It returns whether such pair exist.
	First() bool

	// Last moves the iterator to the last key/value pair. If the iterator
	// only contains one key/value pair then First and Last whould moves
	// to the same key/value pair.
	// It returns whether such pair exist.
	Last() bool

	// Seek moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key.
	// It returns whether such pair exist.
	//
	// It is safe to modify the contents of the argument after Seek returns.
	Seek(key []byte) bool

	// Next moves the iterator to the next key/value pair.
	// It returns whether the iterator is exhausted.
	Next() bool

	// Prev moves the iterator to the previous key/value pair.
	// It returns whether the iterator is exhausted.
	Prev() bool

	// Error returns any accumulated error. Exhausting all the key/value pairs
	// is not considered to be an error.
	Error() error
}

type emptyIterator struct {
	releaser util.Releaser
	released bool
	err      error
}

func (i *emptyIterator) rErr() {
	if i.err == nil && i.released {
		i.err = errors.New("leveldb/iterator: iterator released")
	}
}

func (i *emptyIterator) Release() {
	if i.releaser != nil {
		i.releaser.Release()
		i.releaser = nil
	}
	i.released = true
}

func (i *emptyIterator) SetReleaser(releaser util.Releaser) {
	if !i.released {
		i.releaser = releaser
	}
}

func (*emptyIterator) Valid() bool            { return false }
func (i *emptyIterator) First() bool          { i.rErr(); return false }
func (i *emptyIterator) Last() bool           { i.rErr(); return false }
func (i *emptyIterator) Seek(key []byte) bool { i.rErr(); return false }
func (i *emptyIterator) Next() bool           { i.rErr(); return false }
func (i *emptyIterator) Prev() bool           { i.rErr(); return false }
func (*emptyIterator) Key() []byte            { return nil }
func (*emptyIterator) Value() []byte          { return nil }
func (i *emptyIterator) Error() error         { return i.err }

// NewEmptyIterator creates an empty iterator. The err parameter can be
// nil, but if not nil the given err will be returned by Error method.
func NewEmptyIterator(err error) Iterator {
	return &emptyIterator{err: err}
}
