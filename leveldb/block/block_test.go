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

package block

import (
	"bytes"
	"leveldb/comparer"
	"testing"
)

var cases = []string{
	"a",
	"aa",
	"aaa",
	"aab",
	"aac",
	"aacccc",
	"aacddddddd",
	"aacddddddde",
	"ba",
	"bb",
	"bc",
	"bcc",
	"bcd",
	"bcde",
	"ccde",
	"ccdef",
	"ccdefffffffuuuu",
	"f",
	"fz",
	"zzzz",
}

func TestBlock(t *testing.T) {
	bb := NewWriter(3)
	for _, v := range cases {
		bb.Add([]byte(v), []byte(v))
	}
	ll := bb.Size()
	out := bb.Finish()
	if ll != len(out) {
		t.Error("guessed len doesn't equal with output len, ", ll, "!=", len(out))
	}
	br, err := NewReader(out, comparer.BytesComparer{})
	if err != nil {
		t.Error(err)
	}

	iter := br.NewIterator()

	for i := 1; i < len(cases)+1; i++ {
		for j := 0; j < i; j++ {
			if !iter.Next() {
				t.Error("early eof, err: '%v'", iter.Error())
			}
			if !bytes.Equal(iter.Key(), iter.Value()) {
				t.Error("key and value are not equal, ", iter.Key(), "!=", iter.Value())
			}
			if !bytes.Equal(iter.Key(), []byte(cases[j])) {
				t.Error("key and value are invalid, ", iter.Key(), "!=", []byte(cases[j]))
			}
		}

		for j := i; j > 0; j-- {
			if !iter.Prev() && j > 1 {
				t.Error("early eof, err: '%v'", iter.Error())
			}
			if j > 1 {
				if !bytes.Equal(iter.Key(), iter.Value()) {
					t.Error("key and value are not equal, ", iter.Key(), "!=", iter.Value())
				}
				if !bytes.Equal(iter.Key(), []byte(cases[j-2])) {
					t.Error("key and value are invalid, ", iter.Key(), "!=", []byte(cases[j-2]))
				}
			}
		}
	}

	if !iter.Last() {
		t.Error("early eof")
	}
	if iter.Next() {
		t.Error("should reaching eof")
	}
	for i := 1; i < len(cases)+1; i++ {
		nc := cases[len(cases)-i:]
		for j := i; j > 0; j-- {
			if !iter.Prev() {
				t.Errorf("early eof, err: '%v'", iter.Error())
			}
			if !bytes.Equal(iter.Key(), iter.Value()) {
				t.Error("key and value are not equal, ", iter.Key(), "!=", iter.Value())
			}
			if !bytes.Equal(iter.Key(), []byte(nc[j-1])) {
				t.Error("key and value are invalid, ", iter.Key(), "!=", []byte(cases[j-2]))
			}
		}

		for j := 0; j < i; j++ {
			if !iter.Next() && j < i-1 {
				t.Errorf("early eof, err: '%v'", iter.Error())
			}
			if j < i-1 {
				if !bytes.Equal(iter.Key(), iter.Value()) {
					t.Error("key and value are not equal, ", iter.Key(), "!=", iter.Value())
				}
				if !bytes.Equal(iter.Key(), []byte(nc[j+1])) {
					t.Error("key and value are invalid, ", iter.Key(), "!=", []byte(cases[j]))
				}
			}
		}
	}

	for i := 0; i < len(cases); i++ {
		if !iter.Seek([]byte(cases[i])) {
			t.Errorf("key '%s' is not found, err: '%v'", cases[i], iter.Error())
		}
		if !bytes.Equal(iter.Key(), iter.Value()) {
			t.Error("key and value are not equal, ", iter.Key(), "!=", iter.Value())
		}
		if !bytes.Equal(iter.Key(), []byte(cases[i])) {
			t.Error("key and value are invalid, ", iter.Key(), "!=", []byte(cases[i]))
		}
	}
}
