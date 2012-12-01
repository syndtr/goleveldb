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
	"testing"
	"leveldb"
	"bytes"
	"math/rand"
	"os"
	"time"
)

func randomString(n int) []byte {
	b := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		b.WriteByte(' ' + byte(rand.Intn(95)))
	}
	return b.Bytes()
}

func randomKey(n int) string {
	testChar := []byte{0, 1, 'a', 'b', 'c', 'd', 'e', 0xfd, 0xfe, 0xff}
	b := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		b.WriteByte(testChar[rand.Intn(len(testChar)-1)])
	}
	return string(b.Bytes())
}

type writer struct {
	b *bytes.Buffer
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.b.Write(p)
}

func (w *writer) Sync() error {
	return nil
}

type reader struct {
	r *bytes.Reader
	name string
	size int64
}

func (r *reader) Read(b []byte) (n int, err error) {
	return r.r.Read(b)
}

func (r *reader) ReadAt(b []byte, off int64) (n int, err error) {
	return r.r.ReadAt(b, off)
}

func (r *reader) Seek(offset int64, whence int) (ret int64, err error) {
	panic("not implemented")
	return
}

func (r *reader) Close() error {
	return nil
}

func (r *reader) Stat() (fi os.FileInfo, err error) {
	return r, nil
}

func (r *reader) Name() string {
	return r.name
}

func (r *reader) Size() int64 {
	return r.size
}

func (r *reader) Mode() os.FileMode {
	return os.FileMode(0777)
}

func (r *reader) ModTime() time.Time {
	return time.Now()
}

func (r *reader) IsDir() bool {
	return false
}

func (r *reader) Sys() interface{} {
	return nil
}

type Constructor interface {
	Init() error
	Add(key, value []byte) error
	Finish() (int, error)
	NewIterator() leveldb.Iterator
}

type BlockConstructor struct {
	bb *blockBuilder
	br *block
}

func (t *BlockConstructor) Init() error {
	o := &leveldb.Options{
		BlockRestartInterval: 3,
	}
	t.bb = newBlockBuilder(o, 0)
	return nil
}

func (t *BlockConstructor) Add(key, value []byte) error {
	t.bb.Add(key, value)
	return nil
}

func (t *BlockConstructor) Finish() (size int, err error) {
	buf := t.bb.Finish()
	size = len(buf)
	t.br, err = newBlock(buf)
	return
}

func (t *BlockConstructor) NewIterator() leveldb.Iterator {
	return t.br.NewIterator(leveldb.DefaultComparator)
}

type TableConstructor struct {
	b  *bytes.Buffer
	tb *TableBuilder
	tr *Table
}

func (t *TableConstructor) Init() error {
	t.b = new(bytes.Buffer)
	o := &leveldb.Options{
		BlockSize:            512,
		BlockRestartInterval: 3,
	}
	t.tb = NewTableBuilder(&writer{b: t.b}, o)
	return nil
}

func (t *TableConstructor) Add(key, value []byte) error {
	t.tb.Add(key, value)
	return nil
}

func (t *TableConstructor) Finish() (size int, err error) {
	err = t.tb.Finish()
	if err != nil {
		return
	}
	size = t.b.Len()
	r := &reader{
		r:    bytes.NewReader(t.b.Bytes()),
		name: "table",
		size: int64(t.b.Len()),
	}
	o := &leveldb.Options{
		BlockRestartInterval: 3,
	}
	t.tr, err = NewTable(r, o)
	return
}

func (t *TableConstructor) NewIterator() leveldb.Iterator {
	return t.tr.NewIterator(&leveldb.ReadOptions{})
}

type Harness struct {
	t *testing.T

	keys, values [][]byte
}

func NewHarness(t *testing.T) *Harness {
	return &Harness{t: t}
}

func (h *Harness) Add(key, value []byte) {
	h.keys = append(h.keys, key)
	h.values = append(h.values, value)
}

func (h *Harness) TestAll() {
	h.Test("block", &BlockConstructor{})
	h.Test("table", &TableConstructor{})
}

func (h *Harness) Test(name string, c Constructor) {
	err := c.Init()
	if err != nil {
		h.t.Error("error when initializing constructor:", err.Error())
		return
	}
	for i := range h.keys {
		err = c.Add(h.keys[i], h.values[i])
		if err != nil {
			h.t.Error("error when adding key/value:", err.Error())
			return
		}
	}
	var size int
	size, err = c.Finish()
	if err != nil {
		h.t.Error("error when finishing constructor:", err.Error())
		return
	}

	h.t.Log("final size of the", name, "is:", size, "bytes")

	h.testSimpleScan(c)
}

func (h *Harness) testSimpleScan(c Constructor) {
	iter := c.NewIterator()
	var first, last bool

first:	for i := range h.keys {
		if !iter.Next() {
			h.t.Error("SimpleScan: Forward: unxepected eof")
		}
		if !bytes.Equal(iter.Key(), h.keys[i]) {
			h.t.Error("SimpleScan: Forward: key are invalid, ", string(iter.Key()), "!=", string(h.keys[i]))
		}
		if !bytes.Equal(iter.Value(), h.values[i]) {
			h.t.Error("SimpleScan: Forward: value are invalid, ", iter.Value(), "!=", h.values[i])
		}
	}

	if !first {
		first = true
		if !iter.First() {
			h.t.Error("SimpleScan: ToFirst: unxepected eof")
		}
		if !bytes.Equal(iter.Key(), h.keys[0]) {
			h.t.Error("SimpleScan: ToFirst: key are invalid, ", iter.Key(), "!=", h.keys[0])
		}
		if !bytes.Equal(iter.Value(), h.values[0]) {
			h.t.Error("SimpleScan: ToFirst: value are invalid, ", iter.Value(), "!=", h.values[0])
		}
		if iter.Prev() {
			h.t.Error("SimpleScan: ToFirst: expecting eof")
		}
		goto first
	}

last:	if iter.Next() {
		h.t.Error("SimpleScan: Forward: expecting eof")
	}

	for i := len(h.keys)-1; i >= 0; i-- {
		if !iter.Prev() {
			h.t.Error("SimpleScan: Backward: unxepected eof")
		}
		if !bytes.Equal(iter.Key(), h.keys[i]) {
			h.t.Error("SimpleScan: Backward: key are invalid, ", iter.Key(), "!=", h.keys[i])
		}
		if !bytes.Equal(iter.Value(), h.values[i]) {
			h.t.Error("SimpleScan: Backward: value are invalid, ", iter.Value(), "!=", h.values[i])
		}
	}

	if !last {
		last = true
		if !iter.Last() {
			h.t.Error("SimpleScan: ToLast: unxepected eof")
		}
		i := len(h.keys) - 1
		if !bytes.Equal(iter.Key(), h.keys[i]) {
			h.t.Error("SimpleScan: ToLast: key are invalid, ", iter.Key(), "!=", h.keys[i])
		}
		if !bytes.Equal(iter.Value(), h.values[i]) {
			h.t.Error("SimpleScan: ToLast: value are invalid, ", iter.Value(), "!=", h.values[i])
		}
		goto last
	}

	if iter.Prev() {
		h.t.Error("SimpleScan: Backward: expecting eof")
	}
}

func TestSimpleEmptyKey(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte(""), []byte("v"))
	h.TestAll()
}

func TestSimpleEmptyValue(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("abc"), []byte(""))
	h.Add([]byte("abcd"), []byte(""))
	h.TestAll()
}

func TestSimpleSingle(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("abc"), []byte("v"))
	h.TestAll()
}

func TestSimpleMulti(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("a"), []byte("v"))
	h.Add([]byte("aa"), []byte("v1"))
	h.Add([]byte("aaa"), []byte("v2"))
	h.Add([]byte("aaacccccccccc"), []byte("v2"))
	h.Add([]byte("aaaccccccccccd"), []byte("v3"))
	h.Add([]byte("aaaccccccccccf"), []byte("v4"))
	h.Add([]byte("aaaccccccccccfg"), []byte("v5"))
	h.Add([]byte("ab"), []byte("v6"))
	h.Add([]byte("abc"), []byte("v7"))
	h.Add([]byte("abcd"), []byte("v8"))
	h.Add([]byte("accccccccccccccc"), []byte("v9"))
	h.Add([]byte("b"), []byte("v10"))
	h.Add([]byte("bb"), []byte("v11"))
	h.Add([]byte("bc"), []byte("v12"))
	h.Add([]byte("c"), []byte("v13"))
	h.Add([]byte("c1"), []byte("v13"))
	h.Add([]byte("czzzzzzzzzzzzzz"), []byte("v14"))
	h.Add([]byte("fffffffffffffff"), []byte("v15"))
	h.Add([]byte("g11"), []byte("v15"))
	h.Add([]byte("g111"), []byte("v15"))
	h.Add([]byte("g111\xff"), []byte("v15"))
	h.Add([]byte("zz"), []byte("v16"))
	h.Add([]byte("zzzzzzz"), []byte("v16"))
	h.Add([]byte("zzzzzzzzzzzzzzzz"), []byte("v16"))
	h.TestAll()
}

func TestSimpleSpecialKey(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("\xff\xff"), []byte("v3"))
	h.TestAll()
}

func TestGenerated(t *testing.T) {
	h := NewHarness(t)
	for c := byte('a'); c <= byte('o'); c++ {
		for i := 1; i < 199; i++ {
			key := bytes.Repeat([]byte{c}, i)
			value := randomString(rand.Intn(400))
			h.Add(key, value)
		}
	}
	h.TestAll()
}

func offsetBetween(t *testing.T, v, low, hi uint64) {
	if !(v >= low && v <= hi) {
		t.Errorf("offset %v not in range, want %v - %v", v, low, hi)
	}
}

func TestApproximateOffsetOfPlain(t *testing.T) {
	w := &writer{b: new(bytes.Buffer)}
	o := &leveldb.Options{
		BlockSize:       1024,
		CompressionType: leveldb.NoCompression,
	}
	tb := NewTableBuilder(w, new(leveldb.Options))
	tb.Add([]byte("k01"), []byte("hello"))
	tb.Add([]byte("k02"), []byte("hello2"))
	tb.Add([]byte("k03"), bytes.Repeat([]byte{'x'}, 10000))
	tb.Add([]byte("k04"), bytes.Repeat([]byte{'x'}, 200000))
	tb.Add([]byte("k05"), bytes.Repeat([]byte{'x'}, 300000))
	tb.Add([]byte("k06"), []byte("hello3"))
	tb.Add([]byte("k07"), bytes.Repeat([]byte{'x'}, 100000))
	if err := tb.Finish(); err != nil {
		t.Fatal("error when finalizing table:", err.Error())
	}
	r := &reader{
		r:    bytes.NewReader(w.b.Bytes()),
		name: "table",
		size: int64(w.b.Len()),
	}

	tr, err := NewTable(r, o)
	if err != nil {
		t.Fatal("error when creating table reader instance:", err.Error())
	}

	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k0")),   0,      0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k01a")), 0,      0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k02")),  0,      0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k03")),  0,      0)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k04")),  10000,  11000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k04a")), 210000, 211000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k05")),  210000, 211000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k06")),  510000, 511000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("k07")),  510000, 511000)
	offsetBetween(t, tr.ApproximateOffsetOf([]byte("xyz")),  610000, 612000)
}

