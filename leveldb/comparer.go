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

import "bytes"

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
	FindShortestSeparator(a, b []byte) []byte

	// Changes 'b' to a short string >= 'b'.
	// Simple comparer implementations may return with 'b' unchanged,
	// i.e., an implementation of this method that does nothing is correct.
	// NOTE: Don't modify content of 'b', if modification is necessary
	// copy it first. It is ok to return slice of it.
	FindShortSuccessor(b []byte) []byte
}

var DefaultComparer = BytesComparer{}

type BytesComparer struct{}

func (BytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (BytesComparer) Name() string {
	return "leveldb.BytewiseComparator"
}

func (BytesComparer) FindShortestSeparator(a, b []byte) []byte {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}

	if i >= n {
		// Do not shorten if one string is a prefix of the other
	} else if c := a[i]; c < 0xff && c+1 < b[i] {
		r := make([]byte, i+1)
		copy(r, a)
		r[i]++
		return r
	}
	return a
}

func (BytesComparer) FindShortSuccessor(b []byte) []byte {
	var res []byte
	for _, c := range b {
		if c != 0xff {
			res = append(res, c+1)
			return res
		}
		res = append(res, c)
	}
	return b
}
