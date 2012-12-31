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

package db

import (
	"bytes"
	"leveldb/comparer"
	"testing"
)

var icmp = &iComparer{comparer.BytesComparer{}}

func ikey(key string, seq uint64, t vType) iKey {
	return newIKey([]byte(key), uint64(seq), t)
}

func shortSep(a, b []byte) []byte {
	return icmp.Separator(a, b)
}

func shortSuccessor(b []byte) []byte {
	return icmp.Successor(b)
}

func testSingleKey(t *testing.T, key string, seq uint64, vtype vType) {
	ik := ikey(key, seq, vtype)
	pk := ik.parse()

	if !bytes.Equal(pk.ukey, []byte(key)) {
		t.Errorf("user key does not equal, got %v, want %v", string(pk.ukey), key)
	}

	if pk.seq != seq {
		t.Errorf("seq number does not equal, got %v, want %v", pk.seq, seq)
	}

	if pk.vtype != vtype {
		t.Errorf("type does not equal, got %v, want %v", pk.vtype, vtype)
	}
}

func TestIKey_EncodeDecode(t *testing.T) {
	keys := []string{"", "k", "hello", "longggggggggggggggggggggg"}
	seqs := []uint64{
		1, 2, 3,
		(1 << 8) - 1, 1 << 8, (1 << 8) + 1,
		(1 << 16) - 1, 1 << 16, (1 << 16) + 1,
		(1 << 32) - 1, 1 << 32, (1 << 32) + 1,
	}
	for _, key := range keys {
		for _, seq := range seqs {
			testSingleKey(t, key, seq, tVal)
			testSingleKey(t, "hello", 1, tDel)
		}
	}
}

func assertBytes(t *testing.T, want, got []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("assert failed, got %v, want %v", got, want)
	}
}

func TestIKeyShortSeparator(t *testing.T) {
	// When user keys are same
	assertBytes(t, ikey("foo", 100, tVal),
		shortSep(ikey("foo", 100, tVal),
			ikey("foo", 99, tVal)))
	assertBytes(t, ikey("foo", 100, tVal),
		shortSep(ikey("foo", 100, tVal),
			ikey("foo", 101, tVal)))
	assertBytes(t, ikey("foo", 100, tVal),
		shortSep(ikey("foo", 100, tVal),
			ikey("foo", 100, tVal)))
	assertBytes(t, ikey("foo", 100, tVal),
		shortSep(ikey("foo", 100, tVal),
			ikey("foo", 100, tDel)))

	// When user keys are misordered
	assertBytes(t, ikey("foo", 100, tVal),
		shortSep(ikey("foo", 100, tVal),
			ikey("bar", 99, tVal)))

	// When user keys are different, but correctly ordered
	assertBytes(t, ikey("g", uint64(kMaxSeq), tSeek),
		shortSep(ikey("foo", 100, tVal),
			ikey("hello", 200, tVal)))

	// When start user key is prefix of limit user key
	assertBytes(t, ikey("foo", 100, tVal),
		shortSep(ikey("foo", 100, tVal),
			ikey("foobar", 200, tVal)))

	// When limit user key is prefix of start user key
	assertBytes(t, ikey("foobar", 100, tVal),
		shortSep(ikey("foobar", 100, tVal),
			ikey("foo", 200, tVal)))
}

func TestIKeyShortestSuccessor(t *testing.T) {
	assertBytes(t, ikey("g", uint64(kMaxSeq), tSeek),
		shortSuccessor(ikey("foo", 100, tVal)))
	assertBytes(t, ikey("\xff\xff", 100, tVal),
		shortSuccessor(ikey("\xff\xff", 100, tVal)))
}
