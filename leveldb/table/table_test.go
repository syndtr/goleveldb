// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/testutil"
	"github.com/syndtr/goleveldb/leveldb/util"
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

func offsetBetween(t *testing.T, tr *Reader, key []byte, low, hi int64) {
	v, err := tr.GetApproximateOffset(key)
	if err != nil {
		t.Fatal("GetApproximateOffset returns error: ", err)
		return
	}
	if !(v >= low && v <= hi) {
		t.Errorf("%q : got %v expected in range of %v - %v", key, v, low, hi)
	}
}

func TestGetApproximateOffsetPlain(t *testing.T) {
	w := new(writer)
	o := &opt.Options{
		BlockSize:   1024,
		Compression: opt.NoCompression,
	}
	tw := NewWriter(w, o)
	tw.Append([]byte("k01"), []byte("hello"))
	tw.Append([]byte("k02"), []byte("hello2"))
	tw.Append([]byte("k03"), bytes.Repeat([]byte{'x'}, 10000))
	tw.Append([]byte("k04"), bytes.Repeat([]byte{'x'}, 200000))
	tw.Append([]byte("k05"), bytes.Repeat([]byte{'x'}, 300000))
	tw.Append([]byte("k06"), []byte("hello3"))
	tw.Append([]byte("k07"), bytes.Repeat([]byte{'x'}, 100000))
	if err := tw.Close(); err != nil {
		t.Fatal("error when finalizing table: ", err)
	}
	size := w.Len()
	r := &reader{*bytes.NewReader(w.Bytes())}
	tr := NewReader(r, int64(size), nil, o)

	offsetBetween(t, tr, []byte("k0"), 0, 0)
	offsetBetween(t, tr, []byte("k01a"), 0, 0)
	offsetBetween(t, tr, []byte("k02"), 0, 0)
	offsetBetween(t, tr, []byte("k03"), 0, 0)
	offsetBetween(t, tr, []byte("k04"), 10000, 11000)
	offsetBetween(t, tr, []byte("k04a"), 210000, 211000)
	offsetBetween(t, tr, []byte("k05"), 210000, 211000)
	offsetBetween(t, tr, []byte("k06"), 510000, 511000)
	offsetBetween(t, tr, []byte("k07"), 510000, 511000)
	offsetBetween(t, tr, []byte("xyz"), 610000, 612000)
}

type iteratorTesting struct {
	*testing.T
	testutil.KeyValue
	restartInterval int
	rand            *rand.Rand
}

func (t *iteratorTesting) Build() *block {
	w := &blockWriter{
		restartInterval: t.restartInterval,
		scratch:         make([]byte, 30),
	}
	t.Iterate(func(i int, key, value []byte) {
		w.append(key, value)
	})
	w.finish()
	data := w.buf.Bytes()
	restartsLen := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	t.Logf("block: entries=%d restarts=%d", t.Len(), restartsLen)
	return &block{
		cmp:            comparer.DefaultComparer,
		data:           data,
		restartsLen:    restartsLen,
		restartsOffset: len(data) - (restartsLen+1)*4,
	}
}

func testFirstOrLast(t *testutil.IteratorTesting) {
	bi := t.Iter.(*blockIter)
	if bi.isFirst() != t.IsFirst() {
		t.Errorf("IsFirst: got %v expected %v", bi.isFirst(), t.IsFirst())
	}
	if bi.isLast() != t.IsLast() {
		t.Errorf("IsLast: got %v expected %v", bi.isLast(), t.IsLast())
	}
}

func (t *iteratorTesting) test(b *block, kv testutil.KeyValue, r *util.Range) {
	if b == nil {
		b = t.Build()
	}
	it := testutil.IteratorTesting{
		T:        t.T,
		KeyValue: kv,
		Iter:     b.newIterator(r, false, nil),
		Rand:     t.rand,
		PostFn:   testFirstOrLast,
	}
	it.Test()
	it.Iter.Release()
}

func (t *iteratorTesting) TestFull(b *block) {
	t.test(b, t.Clone(), nil)
}

func (t *iteratorTesting) TestSlice(b *block, start, limit int) {
	r := t.Range(start, limit)
	t.test(b, t.Slice(start, limit), &r)
}

func (t *iteratorTesting) TestRange(b *block, r *util.Range) {
	t.test(b, t.SliceRange(r), r)
}

func (t *iteratorTesting) Test() {
	b := t.Build()
	t.TestFull(b)
	for i := 0; i <= t.Len(); i++ {
		t.TestSlice(b, i, t.Len())
		t.TestSlice(b, 0, i)
	}
	t.IterateInexact(func(i int, key_, key, value []byte) {
		t.TestRange(b, &util.Range{Start: key_, Limit: nil})
		t.TestRange(b, &util.Range{Start: nil, Limit: key_})
	})
	testutil.RandomRange(t.rand, t.Len(), t.Len()*3, func(start, limit int) {
		t.TestSlice(b, start, limit)
	})
}

func newIteratorTesting(t *testing.T, restartIterval int) *iteratorTesting {
	seed := time.Now().UnixNano()
	t.Logf("IteratorTesting: seed %d", seed)
	return &iteratorTesting{
		T:               t,
		restartInterval: restartIterval,
		rand:            rand.New(rand.NewSource(seed)),
	}
}

func TestBlockBasic(t_ *testing.T) {
	for x := 1; x <= 5; x++ {
		t_.Logf("restart_interval=%d", x)
		t := newIteratorTesting(t_, x)
		t.Test()
		t.PutString("", "empty")
		t.Test()
		t.PutString("a1", "foo")
		t.Test()
		t.PutString("a2", "v")
		t.PutString("a3qqwrkks", "hello")
		t.Test()
		t.PutString("a4", "bar")
		t.Test()
		t.PutString("a5111111", "v5")
		t.PutString("a6", "")
		t.Test()
		t.PutString("a7", "v7")
		t.PutString("a8", "vvvvvvvvvvvvvvvvvvvvvv8")
		t.Test()
		t.PutString("b", "v9")
		t.Test()
		t.PutString("c9", "v9")
		t.PutString("c91", "v9")
		t.Test()
		t.PutString("d0", "v9")
		t.Test()

		for c := byte('e'); c <= byte('z'); c++ {
			t.Put(bytes.Repeat([]byte{c}, t.rand.Intn(100)+1), []byte{'v', c})
		}
		t.PutString("\xff\xff", "v\xff")
		t.Test()
	}
}

func TestBlockSliceBeyond(t_ *testing.T) {
	for x := 1; x <= 5; x++ {
		t_.Logf("restart_interval=%d", x)
		t := newIteratorTesting(t_, x)
		t.PutString("k1", "v1")
		t.PutString("k2", "v2")
		t.PutString("k3abcdefgg", "v3")
		t.PutString("k4", "v4")
		t.PutString("k5", "v5")
		b := t.Build()
		t.TestRange(b, &util.Range{Start: []byte("k0"), Limit: []byte("k6")})
		t.TestRange(b, &util.Range{Start: []byte(""), Limit: []byte("zzzzzzz")})
	}
}
