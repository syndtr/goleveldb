// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package comparer provides interface and implementation for ordering
// sets of data.
package comparer

// BasicComparer is the interface that wraps the basic Compare method.
type BasicComparer interface {
	// Three-way comparison.
	//
	// Returns value:
	//   < 0 iff "a" < "b",
	//   == 0 iff "a" == "b",
	//   > 0 iff "a" > "b"
	Compare(a, b []byte) int
}

type Comparer interface {
	BasicComparer

	// The name of the comparer.  Used to check for comparer
	// mismatches (i.e., a DB created with one comparer is
	// accessed using a different comparer.
	//
	// The client of this package should switch to a new name whenever
	// the comparer implementation changes in a way that will cause
	// the relative ordering of any two keys to change.
	//
	// Names starting with "leveldb." are reserved and should not be used
	// by any clients of this package.
	Name() string

	// Advanced functions: these are used to reduce the space requirements
	// for internal data structures like index blocks.

	// If 'a' < 'b', changes 'a' to a short string in [a,b).
	// Simple comparer implementations may return with 'a' unchanged,
	// i.e., an implementation of this method that does nothing is correct.
	// NOTE: Don't modify content of either 'a' or 'b', if modification
	// is necessary copy it first. It is ok to return slice of it.
	Separator(a, b []byte) []byte

	// Changes 'b' to a short string >= 'b'.
	// Simple comparer implementations may return with 'b' unchanged,
	// i.e., an implementation of this method that does nothing is correct.
	// NOTE: Don't modify content of 'b', if modification is necessary
	// copy it first. It is ok to return slice of it.
	Successor(b []byte) []byte
}

// DefaultComparer are default comparer used by LevelDB.
var DefaultComparer = BytesComparer{}
