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
	"testing"
)

func TestBatch(t *testing.T) {
	b1 := new(Batch)
	b1.sequence = 10009
	b1.Put([]byte("key1"), []byte("value1"))
	b1.Put([]byte("key2"), []byte("value2"))
	b1.Delete([]byte("key1"))
	b1.Put([]byte("k"), []byte(""))
	b1.Put([]byte("zzzzzzzzzzz"), []byte("zzzzzzzzzzzzzzzzzzzzzzzz"))
	b1.Delete([]byte("key10000"))
	b1.Delete([]byte("k"))
	buf := b1.encode()
	b2 := new(Batch)
	var n uint32
	err := decodeBatchHeader(buf, &b2.sequence, &n)
	if err != nil {
		t.Error("error when decoding batch header: ", err)
	}
	if b1.sequence != b2.sequence {
		t.Errorf("invalid sequence number want %d, got %d", b1.sequence, b2.sequence)
	}
	if len(b1.rec) != int(n) {
		t.Errorf("invalid record length (in header) want %d, got %d", len(b1.rec), n)
	}
	n2, err := replayBatch(buf, b2)
	if err != nil {
		t.Error("error when replaying batch header: ", err)
	}
	if uint64(n) != n2-b2.sequence {
		t.Errorf("invalid record length (replayBatch ret) want %d, got %d", n, n2-b2.sequence)
	}
	if len(b1.rec) != len(b2.rec) {
		t.Errorf("invalid record length want %d, got %d", len(b1.rec), len(b2.rec))
		return
	}
	for i := range b1.rec {
		r1, r2 := &(b1.rec[i]), &(b2.rec[i])
		if r1.t != r2.t {
			t.Errorf("invalid type on record '%d' want %d, got %d", i, r1.t, r2.t)
		}
		if !bytes.Equal(r1.key, r2.key) {
			t.Errorf("invalid key on record '%d' want %s, got %s", i, string(r1.key), string(r2.key))
		}
		if r1.t == tVal {
			if !bytes.Equal(r1.value, r2.value) {
				t.Errorf("invalid value on record '%d' want %s, got %s", i, string(r1.value), string(r2.value))
			}
		}
	}
}
