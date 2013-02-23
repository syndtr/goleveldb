// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package comparer

import "bytes"

type BytesComparer struct{}

func (BytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (BytesComparer) Name() string {
	return "leveldb.BytewiseComparator"
}

func (BytesComparer) Separator(a, b []byte) []byte {
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

func (BytesComparer) Successor(b []byte) []byte {
	for i, c := range b {
		if c != 0xff {
			r := make([]byte, i+1)
			copy(r, b)
			r[i]++
			return r
		}
	}
	return b
}
