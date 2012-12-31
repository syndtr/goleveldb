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

package memdb

import (
	"leveldb/comparer"
	"testing"
)

func TestPutRemove(t *testing.T) {
	p := New(comparer.BytesComparer{})

	assertExist := func(key string, want bool) {
		got := p.Contains([]byte(key))
		if got != want {
			if got {
				t.Errorf("key %q exist", key)
			} else {
				t.Errorf("key %q doesn't exist", key)
			}
		}
	}

	assertLen := func(want int) {
		got := p.Len()
		if got != want {
			t.Errorf("invalid length, want=%d got=%d", want, got)
		}
	}

	assertLen(0)
	p.Put([]byte("foo"), nil)
	assertLen(1)
	assertExist("foo", true)
	assertExist("bar", false)
	p.Put([]byte("bar"), nil)
	assertLen(2)
	assertExist("bar", true)
	p.Remove([]byte("foo"))
	assertLen(1)
	assertExist("foo", false)
	p.Remove([]byte("foo"))
	assertExist("bar", true)
	p.Put([]byte("zz"), nil)
	assertLen(2)
	assertExist("zz", true)
	p.Remove([]byte("bar"))
	assertExist("bar", false)
	assertExist("zz", true)
	p.Remove([]byte("bar"))
	assertExist("zz", true)
	p.Remove([]byte("zz"))
	assertExist("zz", false)
	assertLen(0)
}
