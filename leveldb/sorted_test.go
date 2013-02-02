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

package leveldb

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"leveldb/block"
	"leveldb/comparer"
	"leveldb/descriptor"
	"leveldb/filter"
	"leveldb/iter"
	"leveldb/memdb"
	"leveldb/opt"
	"leveldb/table"
)

type stConstructor interface {
	init(t *testing.T, ho *stHarnessOpt) error
	add(key, value string) error
	finish() (int, error)
	newIterator() iter.Iterator
	customTest(h *stHarness)
}

type stConstructor_Block struct {
	t *testing.T

	bw *block.Writer
	br *block.Reader
}

func (p *stConstructor_Block) init(t *testing.T, ho *stHarnessOpt) error {
	p.t = t
	p.bw = block.NewWriter(3)
	return nil
}

func (p *stConstructor_Block) add(key, value string) error {
	p.bw.Add([]byte(key), []byte(value))
	return nil
}

func (p *stConstructor_Block) finish() (size int, err error) {
	csize := p.bw.Size()
	buf := p.bw.Finish()

	p.t.Logf("block: contains %d entries and %d restarts", p.bw.Len(), p.bw.CountRestart())

	size = len(buf)
	if csize != size {
		p.t.Errorf("block: calculated size doesn't equal with actual size, %d != %d", csize, size)
	}

	p.br, err = block.NewReader(buf, comparer.BytesComparer{})
	return
}

func (p *stConstructor_Block) newIterator() iter.Iterator {
	return p.br.NewIterator()
}

func (p *stConstructor_Block) customTest(h *stHarness) {}

type stConstructor_Table struct {
	t *testing.T

	file descriptor.File
	w    descriptor.Writer
	r    descriptor.Reader
	tw   *table.Writer
	tr   *table.Reader
}

func (p *stConstructor_Table) init(t *testing.T, ho *stHarnessOpt) error {
	p.t = t

	p.file = newTestDesc(nil).GetFile(0, descriptor.TypeTable)
	p.w, _ = p.file.Create()

	o := &opt.Options{
		BlockSize:            512,
		BlockRestartInterval: 3,
	}
	p.tw = table.NewWriter(p.w, o)
	return nil
}

func (p *stConstructor_Table) add(key, value string) error {
	p.tw.Add([]byte(key), []byte(value))
	return nil
}

func (p *stConstructor_Table) finish() (size int, err error) {
	p.t.Logf("table: contains %d entries and %d blocks", p.tw.Len(), p.tw.CountBlock())

	err = p.tw.Finish()
	if err != nil {
		return
	}
	p.w.Close()

	tsize := uint64(p.tw.Size())

	fsize, _ := p.file.Size()
	if fsize != tsize {
		p.t.Errorf("table: calculated size doesn't equal with actual size, calculated=%d actual=%d", tsize, fsize)
	}

	p.r, _ = p.file.Open()
	o := &opt.Options{
		BlockRestartInterval: 3,
		Filter:               filter.NewBloomFilter(10),
	}
	p.tr, err = table.NewReader(p.r, fsize, o, nil)
	return int(fsize), nil
}

func (p *stConstructor_Table) newIterator() iter.Iterator {
	return p.tr.NewIterator(&opt.ReadOptions{})
}

func (p *stConstructor_Table) customTest(h *stHarness) {
	ro := &opt.ReadOptions{}
	for i := range h.keys {
		rkey, rval, err := p.tr.Get([]byte(h.keys[i]), ro)
		if err != nil {
			h.t.Error("table: CustomTest: Get: error: ", err)
			continue
		}
		if string(rkey) != h.keys[i] {
			h.t.Errorf("table: CustomTest: Get: key are invalid, got=%q want=%q",
				shorten(string(rkey)), shorten(h.keys[i]))
		}
		if string(rval) != h.values[i] {
			h.t.Errorf("table: CustomTest: Get: value are invalid, got=%q want=%q",
				shorten(string(rval)), shorten(h.values[i]))
		}
	}
}

type stConstructor_MemDB struct {
	t *testing.T

	mem *memdb.DB
}

func (p *stConstructor_MemDB) init(t *testing.T, ho *stHarnessOpt) error {
	ho.Randomize = true
	p.t = t
	p.mem = memdb.New(comparer.BytesComparer{})
	return nil
}

func (p *stConstructor_MemDB) add(key, value string) error {
	p.mem.Put([]byte(key), []byte(value))
	return nil
}

func (p *stConstructor_MemDB) finish() (size int, err error) {
	return int(p.mem.Size()), nil
}

func (p *stConstructor_MemDB) newIterator() iter.Iterator {
	return p.mem.NewIterator()
}

func (p *stConstructor_MemDB) customTest(h *stHarness) {}

type stConstructor_MergedMemDB struct {
	t *testing.T

	mem [3]*memdb.DB
}

func (p *stConstructor_MergedMemDB) init(t *testing.T, ho *stHarnessOpt) error {
	ho.Randomize = true
	p.t = t
	for i := range p.mem {
		p.mem[i] = memdb.New(comparer.BytesComparer{})
	}
	return nil
}

func (p *stConstructor_MergedMemDB) add(key, value string) error {
	p.mem[rand.Intn(99999)%3].Put([]byte(key), []byte(value))
	return nil
}

func (p *stConstructor_MergedMemDB) finish() (size int, err error) {
	for i, m := range p.mem {
		p.t.Logf("merged: memdb[%d] size: %d", i, m.Size())
		size += m.Size()
	}
	return
}

func (p *stConstructor_MergedMemDB) newIterator() iter.Iterator {
	var its []iter.Iterator
	for _, m := range p.mem {
		its = append(its, m.NewIterator())
	}
	return iter.NewMergedIterator(its, comparer.BytesComparer{})
}

func (p *stConstructor_MergedMemDB) customTest(h *stHarness) {}

type stConstructor_DB struct {
	t *testing.T

	desc *testDesc
	ro   *opt.ReadOptions
	wo   *opt.WriteOptions
	db   *DB
}

func (p *stConstructor_DB) init(t *testing.T, ho *stHarnessOpt) (err error) {
	ho.Randomize = true
	p.t = t
	p.desc = newTestDesc(nil)
	o := &opt.Options{
		Flag:        opt.OFCreateIfMissing,
		WriteBuffer: 2800,
	}
	p.ro = &opt.ReadOptions{}
	p.wo = &opt.WriteOptions{}
	p.db, err = Open(p.desc, o)
	return
}

func (p *stConstructor_DB) add(key, value string) error {
	p.db.Put([]byte(key), []byte(value), p.wo)
	return nil
}

func (p *stConstructor_DB) finish() (size int, err error) {
	return p.desc.Sizes(), nil
}

func (p *stConstructor_DB) newIterator() iter.Iterator {
	return p.db.NewIterator(p.ro)
}

func (p *stConstructor_DB) customTest(h *stHarness) {
	p.db.Close()
	p.db = nil
	runtime.GC()
}

type stHarnessOpt struct {
	Randomize bool
}

type stHarness struct {
	t *testing.T

	keys, values []string
}

func newStHarness(t *testing.T) *stHarness {
	return &stHarness{t: t}
}

func (h *stHarness) add(key, value string) {
	h.keys = append(h.keys, key)
	h.values = append(h.values, value)
}

func (h *stHarness) testAll() {
	h.test("block", &stConstructor_Block{})
	h.test("table", &stConstructor_Table{})
	h.test("memdb", &stConstructor_MemDB{})
	h.test("merged", &stConstructor_MergedMemDB{})
	h.test("db", &stConstructor_DB{})
}

func (h *stHarness) test(name string, c stConstructor) {
	ho := new(stHarnessOpt)

	err := c.init(h.t, ho)
	if err != nil {
		h.t.Error("error when initializing constructor:", err.Error())
		return
	}

	keys, values := h.keys, h.values
	if ho.Randomize {
		m := len(h.keys)
		times := m * 2
		r1, r2 := rand.New(rand.NewSource(0xdeadbeef)), rand.New(rand.NewSource(0xbeefface))
		keys, values = make([]string, m), make([]string, m)
		copy(keys, h.keys)
		copy(values, h.values)
		for n := 0; n < times; n++ {
			i, j := r1.Intn(99999)%m, r2.Intn(99999)%m
			if i == j {
				continue
			}
			keys[i], keys[j] = keys[j], keys[i]
			values[i], values[j] = values[j], values[i]
		}
	}

	for i := range keys {
		err = c.add(keys[i], values[i])
		if err != nil {
			h.t.Error("error when adding key/value:", err.Error())
			return
		}
	}

	var size int
	size, err = c.finish()
	if err != nil {
		h.t.Error("error when finishing constructor:", err.Error())
		return
	}

	h.t.Logf(name+": final size is %d bytes", size)
	h.testScan(name, c)
	h.testSeek(name, c)
	c.customTest(h)
	h.t.Log(name + ": test is done")
}

func (h *stHarness) testScan(name string, c stConstructor) {
	it := c.newIterator()

	for i := 0; i < 3; i++ {
		if it.Prev() {
			h.t.Errorf(name+": SortedTest: Scan: Backward: expecting eof (it=%d)", i)
		} else if it.Valid() {
			h.t.Errorf(name+": SortedTest: Scan: Backward: Valid != false (it=%d)", i)
		}
	}

	it = c.newIterator()
	var first, last bool

first:
	for i := range h.keys {
		if !it.Next() {
			h.t.Error(name + ": SortedTest: Scan: Forward: unxepected eof")
		} else if !it.Valid() {
			h.t.Error(name + ": SortedTest: Scan: Forward: Valid != true")
		}
		rkey, rval := string(it.Key()), string(it.Value())
		if rkey != h.keys[i] {
			h.t.Errorf(name+": SortedTest: Scan: Forward: key are invalid, got=%q want=%q",
				shorten(rkey), shorten(h.keys[i]))
		}
		if rval != h.values[i] {
			h.t.Errorf(name+": SortedTest: Scan: Forward: value are invalid, got=%q want=%q",
				shorten(rval), shorten(h.values[i]))
		}
	}

	return

	if !first {
		first = true
		if !it.First() {
			h.t.Error(name + ": SortedTest: Scan: ToFirst: unxepected eof")
		} else if !it.Valid() {
			h.t.Error(name + ": SortedTest: Scan: ToFirst: Valid != true")
		}
		rkey, rval := string(it.Key()), string(it.Value())
		if rkey != h.keys[0] {
			h.t.Errorf(name+": SortedTest: Scan: ToFirst: key are invalid, got=%q want=%q",
				shorten(rkey), shorten(h.keys[0]))
		}
		if rval != h.values[0] {
			h.t.Errorf(name+": SortedTest: Scan: ToFirst: value are invalid, got=%q want=%q",
				shorten(rval), shorten(h.values[0]))
		}
		if it.Prev() {
			h.t.Error(name + ": SortedTest: Scan: ToFirst: expecting eof")
		} else if it.Valid() {
			h.t.Error(name + ": SortedTest: Scan: ToFirst: Valid != false")
		}
		goto first
	}

last:
	for i := 0; i < 3; i++ {
		if it.Next() {
			h.t.Errorf(name+": SortedTest: Scan: Forward: expecting eof (it=%d)", i)
		} else if it.Valid() {
			h.t.Errorf(name+": SortedTest: Scan: Forward: Valid != false (it=%d)", i)
		}
	}

	for i := len(h.keys) - 1; i >= 0; i-- {
		if !it.Prev() {
			h.t.Error(name + ": SortedTest: Scan: Backward: unxepected eof")
		} else if !it.Valid() {
			h.t.Error(name + ": SortedTest: Scan: Backward: Valid != true")
		}
		rkey, rval := string(it.Key()), string(it.Value())
		if rkey != h.keys[i] {
			h.t.Errorf(name+": SortedTest: Scan: Backward: key are invalid, got=%q want=%q",
				shorten(rkey), shorten(h.keys[i]))
		}
		if rval != h.values[i] {
			h.t.Errorf(name+": SortedTest: Scan: Backward: value are invalid, got=%q want=%q",
				shorten(rval), shorten(h.values[i]))
		}
	}

	if !last {
		last = true
		if !it.Last() {
			h.t.Error(name + ": SortedTest: Scan: ToLast: unxepected eof")
		} else if !it.Valid() {
			h.t.Error(name + ": SortedTest: Scan: ToLast: Valid != true")
		}
		i := len(h.keys) - 1
		rkey, rval := string(it.Key()), string(it.Value())
		if rkey != h.keys[i] {
			h.t.Errorf(name+": SortedTest: Scan: ToLast: key are invalid, got=%q want=%q",
				shorten(rkey), shorten(h.keys[i]))
		}
		if rval != h.values[i] {
			h.t.Errorf(name+": SortedTest: Scan: ToLast: value are invalid, got=%q want=%q",
				shorten(rval), shorten(h.values[i]))
		}
		goto last
	}

	for i := 0; i < 3; i++ {
		if it.Prev() {
			h.t.Errorf(name+": SortedTest: Scan: Backward: expecting eof (it=%d)", i)
		} else if it.Valid() {
			h.t.Errorf(name+": SortedTest: Scan: Backward: Valid != false (it=%d)", i)
		}
	}
}

func (h *stHarness) testSeek(name string, c stConstructor) {
	it := c.newIterator()

	for i, key := range h.keys {
		if !it.Seek([]byte(key)) {
			h.t.Errorf(name+": SortedTest: Seek: key %q is not found, err: %v",
				shorten(key), it.Error())
			continue
		} else if !it.Valid() {
			h.t.Error(name + ": SortedTest: Seek: Valid != true")
		}

		for j := i; j >= 0; j-- {
			rkey, rval := string(it.Key()), string(it.Value())
			if rkey != h.keys[j] {
				h.t.Errorf(name+": SortedTest: Seek: key are invalid, got=%q want=%q",
					shorten(rkey), shorten(h.keys[j]))
			}
			if rval != h.values[j] {
				h.t.Errorf(name+": SortedTest: Seek: value are invalid, got=%q want=%q",
					shorten(rval), shorten(h.values[j]))
			}
			ret := it.Prev()
			if j == 0 && ret {
				h.t.Error(name + ": SortedTest: Seek: Backward: expecting eof")
			} else if j > 0 && !ret {
				h.t.Error(name+": SortedTest: Seek: Backward: unxepected eof, err: ", it.Error())
			}
		}
	}
}

func TestSorted_EmptyKey(t *testing.T) {
	h := newStHarness(t)
	h.add("", "v")
	h.testAll()
}

func TestSorted_EmptyValue(t *testing.T) {
	h := newStHarness(t)
	h.add("abc", "")
	h.add("abcd", "")
	h.testAll()
}

func TestSorted_Single(t *testing.T) {
	h := newStHarness(t)
	h.add("abc", "v")
	h.testAll()
}

func TestSorted_Multi(t *testing.T) {
	h := newStHarness(t)
	h.add("a", "v")
	h.add("aa", "v1")
	h.add("aaa", "v2")
	h.add("aaacccccccccc", "v2")
	h.add("aaaccccccccccd", "v3")
	h.add("aaaccccccccccf", "v4")
	h.add("aaaccccccccccfg", "v5")
	h.add("ab", "v6")
	h.add("abc", "v7")
	h.add("abcd", "v8")
	h.add("accccccccccccccc", "v9")
	h.add("b", "v10")
	h.add("bb", "v11")
	h.add("bc", "v12")
	h.add("c", "v13")
	h.add("c1", "v13")
	h.add("czzzzzzzzzzzzzz", "v14")
	h.add("fffffffffffffff", "v15")
	h.add("g11", "v15")
	h.add("g111", "v15")
	h.add("g111\xff", "v15")
	h.add("zz", "v16")
	h.add("zzzzzzz", "v16")
	h.add("zzzzzzzzzzzzzzzz", "v16")
	h.testAll()
}

func TestSorted_SpecialKey(t *testing.T) {
	h := newStHarness(t)
	h.add("\xff\xff", "v3")
	h.testAll()
}

func TestSorted_GeneratedShort(t *testing.T) {
	h := newStHarness(t)
	h.add("", "v")
	n := 0
	for c := byte('a'); c <= byte('o'); c++ {
		for i := 1; i < 10; i++ {
			key := bytes.Repeat([]byte{c}, i)
			h.add(string(key), "v"+fmt.Sprint(n))
			n++
		}
	}
	h.testAll()
}

func TestSorted_GeneratedLong(t *testing.T) {
	h := newStHarness(t)
	n := 0
	for c := byte('a'); c <= byte('o'); c++ {
		for i := 150; i < 180; i++ {
			key := bytes.Repeat([]byte{c}, i)
			h.add(string(key), "v"+fmt.Sprint(n))
			n++
		}
	}
	h.testAll()
}
