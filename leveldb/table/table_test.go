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

package table

import (
	"bytes"
	"leveldb"
	"testing"
)

type writer struct {
	bytes.Buffer
}

func (*writer) Close() error { return nil }
func (*writer) Sync() error  { return nil }

type reader struct {
	bytes.Reader
}

func (*reader) Close() error { return nil }

func offsetBetween(t *testing.T, v, low, hi uint64) {
	if !(v >= low && v <= hi) {
		t.Errorf("offset %v not in range, want %v - %v", v, low, hi)
	}
}

func TestApproximateOffsetOfPlain(t *testing.T) {
	w := new(writer)
	o := &leveldb.Options{
		BlockSize:       1024,
		CompressionType: leveldb.NoCompression,
	}
	tw := NewWriter(w, new(leveldb.Options))
	tw.Add([]byte("k01"), []byte("hello"))
	tw.Add([]byte("k02"), []byte("hello2"))
	tw.Add([]byte("k03"), bytes.Repeat([]byte{'x'}, 10000))
	tw.Add([]byte("k04"), bytes.Repeat([]byte{'x'}, 200000))
	tw.Add([]byte("k05"), bytes.Repeat([]byte{'x'}, 300000))
	tw.Add([]byte("k06"), []byte("hello3"))
	tw.Add([]byte("k07"), bytes.Repeat([]byte{'x'}, 100000))
	if err := tw.Finish(); err != nil {
		t.Fatal("error when finalizing table:", err.Error())
	}
	size := w.Len()
	r := &reader{*bytes.NewReader(w.Bytes())}
	tr, err := NewReader(r, uint64(size), o, nil)
	if err != nil {
		t.Fatal("error when creating table reader instance:", err.Error())
	}

	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k0")), 0, 0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k01a")), 0, 0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k02")), 0, 0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k03")), 0, 0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k04")), 10000, 11000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k04a")), 210000, 211000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k05")), 210000, 211000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k06")), 510000, 511000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k07")), 510000, 511000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("xyz")), 610000, 612000)
}
