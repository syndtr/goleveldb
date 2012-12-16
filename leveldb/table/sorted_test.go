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
	"leveldb/block"
	"leveldb/memdb"
	"math/rand"
	"os"
	"testing"
	"time"
)

func randomString(n int) []byte {
	b := new(bytes.Buffer)
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(' ' + byte(rand.Intn(95)))
	}
	return b.Bytes()
}

func randomKey(n int) string {
	testChar := []byte{0, 1, 'a', 'b', 'c', 'd', 'e', 0xfd, 0xfe, 0xff}
	b := new(bytes.Buffer)
	b.Grow(n)
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

func (w *writer) Close() error {
	return nil
}

type reader struct {
	r    *bytes.Reader
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
	Finish(t *testing.T) (int, error)
	NewIterator() leveldb.Iterator
	CustomTest(h *Harness)
}

type BlockConstructor struct {
	bw *block.Writer
	br *block.Reader
}

func (p *BlockConstructor) Init() error {
	p.bw = block.NewWriter(3)
	return nil
}

func (p *BlockConstructor) Add(key, value []byte) error {
	p.bw.Add(key, value)
	return nil
}

func (p *BlockConstructor) Finish(t *testing.T) (size int, err error) {
	csize := p.bw.Size()
	buf := p.bw.Finish()
	t.Logf("block: contains %d entries and %d restarts", p.bw.Len(), p.bw.CountRestart())
	size = len(buf)
	if csize != size {
		t.Errorf("block: calculated size doesn't equal with actual size, %d != %d", csize, size)
	}
	p.br, err = block.NewReader(buf)
	return
}

func (p *BlockConstructor) NewIterator() leveldb.Iterator {
	return p.br.NewIterator(leveldb.DefaultComparator)
}

func (p *BlockConstructor) CustomTest(h *Harness) {}

type TableConstructor struct {
	b  *bytes.Buffer
	tw *Writer
	tr *Reader
}

func (p *TableConstructor) Init() error {
	p.b = new(bytes.Buffer)
	o := &leveldb.Options{
		BlockSize:            512,
		BlockRestartInterval: 3,
	}
	p.tw = NewWriter(&writer{b: p.b}, o)
	return nil
}

func (p *TableConstructor) Add(key, value []byte) error {
	p.tw.Add(key, value)
	return nil
}

func (p *TableConstructor) Finish(t *testing.T) (size int, err error) {
	t.Logf("table: contains %d entries and %d blocks", p.tw.Len(), p.tw.CountBlock())
	err = p.tw.Finish()
	if err != nil {
		return
	}
	size = p.b.Len()
	r := &reader{
		r:    bytes.NewReader(p.b.Bytes()),
		name: "table",
		size: int64(size),
	}
	o := &leveldb.Options{
		BlockRestartInterval: 3,
		FilterPolicy:         leveldb.NewBloomFilter(10),
	}
	p.tr, err = NewReader(r, uint64(size), o, 0)
	return
}

func (p *TableConstructor) NewIterator() leveldb.Iterator {
	return p.tr.NewIterator(&leveldb.ReadOptions{})
}

func (p *TableConstructor) CustomTest(h *Harness) {
	ro := &leveldb.ReadOptions{}
	for i := range h.keys {
		rkey, rval, err := p.tr.Get(h.keys[i], ro)
		if err != nil {
			h.t.Errorf("table: CustomTest: Get: error '%v'", err)
			continue
		}
		if !bytes.Equal(rkey, h.keys[i]) {
			h.t.Error("table: CustomTest: Get: key are invalid, ", rkey, "!=", h.keys[i])
		}
		if !bytes.Equal(rval, h.values[i]) {
			h.t.Error("table: CustomTest: Get: value are invalid, ", rval, "!=", h.values[i])
		}
	}
}

type MemDBConstructor struct {
	mem *memdb.DB
}

func (p *MemDBConstructor) Init() error {
	p.mem = memdb.New(leveldb.DefaultComparator)
	return nil
}

func (p *MemDBConstructor) Add(key, value []byte) error {
	p.mem.Put(key, value)
	return nil
}

func (p *MemDBConstructor) Finish(t *testing.T) (size int, err error) {
	return int(p.mem.ApproxMemSize()), nil
}

func (p *MemDBConstructor) NewIterator() leveldb.Iterator {
	return p.mem.NewIterator()
}

func (p *MemDBConstructor) CustomTest(h *Harness) {}

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
	h.Test("memdb", &MemDBConstructor{})
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
	size, err = c.Finish(h.t)
	if err != nil {
		h.t.Error("error when finishing constructor:", err.Error())
		return
	}

	h.t.Logf(name+": final size is %d bytes", size)
	h.testScan(name, c)
	h.testSeek(name, c)
	c.CustomTest(h)
	h.t.Log(name + ": test is done")
}

func (h *Harness) testScan(name string, c Constructor) {
	iter := c.NewIterator()
	var first, last bool

first:
	for i := range h.keys {
		if !iter.Next() {
			h.t.Error(name + ": SortedTest: Scan: Forward: unxepected eof")
		}
		if !bytes.Equal(iter.Key(), h.keys[i]) {
			h.t.Error(name+": SortedTest: Scan: Forward: key are invalid, ", iter.Key(), "!=", h.keys[i])
		}
		if !bytes.Equal(iter.Value(), h.values[i]) {
			h.t.Error(name+": SortedTest: Scan: Forward: value are invalid, ", iter.Value(), "!=", h.values[i])
		}
	}

	if !first {
		first = true
		if !iter.First() {
			h.t.Error(name + ": SortedTest: Scan: ToFirst: unxepected eof")
		}
		if !bytes.Equal(iter.Key(), h.keys[0]) {
			h.t.Error(name+": SortedTest: Scan: ToFirst: key are invalid, ", iter.Key(), "!=", h.keys[0])
		}
		if !bytes.Equal(iter.Value(), h.values[0]) {
			h.t.Error(name+": SortedTest: Scan: ToFirst: value are invalid, ", iter.Value(), "!=", h.values[0])
		}
		if iter.Prev() {
			h.t.Error(name + ": SortedTest: Scan: ToFirst: expecting eof")
		}
		goto first
	}

last:
	if iter.Next() {
		h.t.Error(name + ": SortedTest: Scan: Forward: expecting eof")
	}

	for i := len(h.keys) - 1; i >= 0; i-- {
		if !iter.Prev() {
			h.t.Error(name + ": SortedTest: Scan: Backward: unxepected eof")
		}
		if !bytes.Equal(iter.Key(), h.keys[i]) {
			h.t.Error(name+": SortedTest: Scan: Backward: key are invalid, ", iter.Key(), "!=", h.keys[i])
		}
		if !bytes.Equal(iter.Value(), h.values[i]) {
			h.t.Error(name+": SortedTest: Scan: Backward: value are invalid, ", iter.Value(), "!=", h.values[i])
		}
	}

	if !last {
		last = true
		if !iter.Last() {
			h.t.Error(name + ": SortedTest: Scan: ToLast: unxepected eof")
		}
		i := len(h.keys) - 1
		if !bytes.Equal(iter.Key(), h.keys[i]) {
			h.t.Error(name+": SortedTest: Scan: ToLast: key are invalid, ", iter.Key(), "!=", h.keys[i])
		}
		if !bytes.Equal(iter.Value(), h.values[i]) {
			h.t.Error(name+": SortedTest: Scan: ToLast: value are invalid, ", iter.Value(), "!=", h.values[i])
		}
		goto last
	}

	if iter.Prev() {
		h.t.Error(name + ": SortedTest: Scan: Backward: expecting eof")
	}
}

func (h *Harness) testSeek(name string, c Constructor) {
	iter := c.NewIterator()

	for i, key := range h.keys {
		if !iter.Seek(key) {
			h.t.Errorf(name+": SortedTest: Seek: Forward: key '%v' is not found, err: '%v'", key, iter.Error())
			continue
		}
		for j := i; j >= 0; j-- {
			key, value := h.keys[j], h.values[j]
			if !bytes.Equal(iter.Key(), key) {
				h.t.Errorf(name+": SortedTest: Seek: Forward: key are invalid, want %v, got %v", key, iter.Key())
			}
			if !bytes.Equal(iter.Value(), value) {
				h.t.Errorf(name+": SortedTest: Seek: Forward: value are invalid, want %v, got %v", value, iter.Value())
			}
			ret := iter.Prev()
			if j == 0 && ret {
				h.t.Error(name + ": SortedTest: Seek: Forward: expecting eof")
			} else if j > 0 && !ret {
				h.t.Error(name+": SortedTest: Seek: Forward: unxepected eof, err: ", iter.Error())
			}
		}
	}
}

func TestSorted_EmptyKey(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte(""), []byte("v"))
	h.TestAll()
}

func TestSorted_EmptyValue(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("abc"), []byte(""))
	h.Add([]byte("abcd"), []byte(""))
	h.TestAll()
}

func TestSorted_Single(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("abc"), []byte("v"))
	h.TestAll()
}

func TestSorted_Multi(t *testing.T) {
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

func TestSorted_SpecialKey(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("\xff\xff"), []byte("v3"))
	h.TestAll()
}

func TestSorted_GeneratedShort(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte(""), []byte("v"))
	for c := byte('a'); c <= byte('o'); c++ {
		for i := 1; i < 10; i++ {
			key := bytes.Repeat([]byte{c}, i)
			h.Add(key, []byte(""))
		}
	}
	h.TestAll()
}

func TestSorted_GeneratedLong(t *testing.T) {
	h := NewHarness(t)
	for c := byte('a'); c <= byte('o'); c++ {
		for i := 150; i < 180; i++ {
			key := bytes.Repeat([]byte{c}, i)
			value := randomString(rand.Intn(400))
			h.Add(key, value)
		}
	}
	h.TestAll()
}
