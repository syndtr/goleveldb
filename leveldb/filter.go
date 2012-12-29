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

import "io"

type Filter interface {
	// Return the name of this policy.  Note that if the filter encoding
	// changes in an incompatible way, the name returned by this method
	// must be changed.  Otherwise, old incompatible filters may be
	// passed to methods of this type.
	Name() string

	// keys[0,n-1] contains a list of keys (potentially with duplicates)
	// that are ordered according to the user supplied comparer.
	// Return a filter that summarizes keys[0,n-1].
	CreateFilter(keys [][]byte, buf io.Writer)

	// "filter" contains the data appended by a preceding call to
	// CreateFilter() on this class.  This method must return true if
	// the key was in the list of keys passed to CreateFilter().
	// This method may return true or false if the key was not on the
	// list, but it should aim to return false with a high probability.
	KeyMayMatch(key, filter []byte) bool
}
