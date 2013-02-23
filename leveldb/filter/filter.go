// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package filter provides interface and implementation of probabilistic
// data structure.
//
// The filter is resposible for creating small filter from a set of a key.
// These filter will then used to test whether a key is a member of a set.
// In many cases, a filter can cut down the number of disk seeks form a
// handful to a single disk seek per DB.Get() call.
//
// Most people will want to use the builtin bloom filter support.
package filter

import "io"

type Filter interface {
	// Return the name of this policy.  Note that if the filter encoding
	// changes in an incompatible way, the name returned by this method
	// must be changed.  Otherwise, old incompatible filters may be
	// passed to methods of this type.
	Name() string

	// keys[0,n-1] contains a list of keys (potentially with duplicates)
	// that are ordered according to the user supplied comparer.
	// Write the filter that summarizes keys[0,n-1] to buf.
	CreateFilter(keys [][]byte, buf io.Writer)

	// "filter" contains the data appended by a preceding call to
	// CreateFilter() on this class.  This method must return true if
	// the key was in the list of keys passed to CreateFilter().
	// This method may return true or false if the key was not on the
	// list, but it should aim to return false with a high probability.
	KeyMayMatch(key, filter []byte) bool
}
