// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package iterator provides interface and implementation to traverse over
// contents of a database.
package iterator

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

	// Release any resources associated with the iterator. It is valid to
	// call Release multiple times. Other method should not be called after
	// the iterator has been released.
	Release()

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
func (*EmptyIterator) Release()             {}
func (i *EmptyIterator) Error() error       { return i.Err }
