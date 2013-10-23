// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/table"
)

type stConstructor interface {
	init(t *testing.T, ho *stHarnessOpt) error
	add(key, value string) error
	finish() (int, error)
	newIterator() iterator.Iterator
	customTest(h *stHarness)
	close()
}

type stConstructor_Table struct {
	t      *testing.T
	buf    bytes.Buffer
	o      *opt.Options
	writer *table.Writer
	reader *table.Reader
}

func (tc *stConstructor_Table) init(t *testing.T, ho *stHarnessOpt) error {
	tc.t = t
	tc.o = &opt.Options{
		BlockSize:            512,
		BlockRestartInterval: 3,
	}
	tc.writer = table.NewWriter(&tc.buf, tc.o)
	return nil
}

func (tc *stConstructor_Table) add(key, value string) error {
	return tc.writer.Append([]byte(key), []byte(value))
}

func (tc *stConstructor_Table) finish() (size int, err error) {
	err = tc.writer.Close()
	if err != nil {
		return
	}
	tc.t.Logf("table: contains %d entries and %d blocks", tc.writer.EntriesLen(), tc.writer.BlocksLen())
	size = tc.buf.Len()
	if csize := int(tc.writer.BytesLen()); csize != size {
		tc.t.Errorf("table: invalid calculated size, calculated=%d actual=%d", csize, size)
	}
	tc.reader = table.NewReader(bytes.NewReader(tc.buf.Bytes()), int64(size), nil, tc.o)
	return
}

func (tc *stConstructor_Table) newIterator() iterator.Iterator {
	return tc.reader.NewIterator(nil)
}

func (tc *stConstructor_Table) customTest(h *stHarness) {
	for i := range h.keys {
		key, value, err := tc.reader.Find([]byte(h.keys[i]), nil)
		if err != nil {
			h.t.Errorf("table: CustomTest: Find: %v", err)
			continue
		}
		if string(key) != h.keys[i] {
			h.t.Errorf("table: CustomTest: Find: invalid key, got=%q want=%q",
				shorten(string(key)), shorten(h.keys[i]))
		}
		if string(value) != h.values[i] {
			h.t.Errorf("table: CustomTest: Find: invalid value, got=%q want=%q",
				shorten(string(value)), shorten(h.values[i]))
		}
	}
}

func (tc *stConstructor_Table) close() {}

type stConstructor_MemDB struct {
	t  *testing.T
	db *memdb.DB
}

func (mc *stConstructor_MemDB) init(t *testing.T, ho *stHarnessOpt) error {
	ho.Randomize = true
	mc.t = t
	mc.db = memdb.New(comparer.DefaultComparer, 0)
	return nil
}

func (mc *stConstructor_MemDB) add(key, value string) error {
	mc.db.Put([]byte(key), []byte(value))
	return nil
}

func (mc *stConstructor_MemDB) finish() (size int, err error) {
	return int(mc.db.Size()), nil
}

func (mc *stConstructor_MemDB) newIterator() iterator.Iterator {
	return mc.db.NewIterator()
}

func (mc *stConstructor_MemDB) customTest(h *stHarness) {}
func (mc *stConstructor_MemDB) close()                  {}

type stConstructor_MergedMemDB struct {
	t  *testing.T
	db [3]*memdb.DB
}

func (mc *stConstructor_MergedMemDB) init(t *testing.T, ho *stHarnessOpt) error {
	ho.Randomize = true
	mc.t = t
	for i := range mc.db {
		mc.db[i] = memdb.New(comparer.DefaultComparer, 0)
	}
	return nil
}

func (mc *stConstructor_MergedMemDB) add(key, value string) error {
	mc.db[rand.Intn(99999)%len(mc.db)].Put([]byte(key), []byte(value))
	return nil
}

func (mc *stConstructor_MergedMemDB) finish() (size int, err error) {
	for i, db := range mc.db {
		mc.t.Logf("merged: db[%d] size: %d", i, db.Size())
		size += db.Size()
	}
	return
}

func (mc *stConstructor_MergedMemDB) newIterator() iterator.Iterator {
	var ii []iterator.Iterator
	for _, db := range mc.db {
		ii = append(ii, db.NewIterator())
	}
	return iterator.NewMergedIterator(ii, comparer.DefaultComparer, true)
}

func (mc *stConstructor_MergedMemDB) customTest(h *stHarness) {}
func (mc *stConstructor_MergedMemDB) close()                  {}

type stConstructor_DB struct {
	t    *testing.T
	stor *testStorage
	ro   *opt.ReadOptions
	wo   *opt.WriteOptions
	db   *DB
}

func (dc *stConstructor_DB) init(t *testing.T, ho *stHarnessOpt) (err error) {
	ho.Randomize = true
	dc.t = t
	dc.stor = newTestStorage(t)
	dc.ro = nil
	dc.wo = nil
	dc.db, err = Open(dc.stor, &opt.Options{
		WriteBuffer: 2800,
	})
	if err != nil {
		dc.stor.Close()
	}
	return
}

func (dc *stConstructor_DB) add(key, value string) error {
	return dc.db.Put([]byte(key), []byte(value), dc.wo)
}

func (dc *stConstructor_DB) finish() (size int, err error) {
	iter := dc.db.NewIterator(dc.ro)
	defer iter.Release()
	var r Range
	if iter.First() {
		r.Start = append([]byte{}, iter.Key()...)
	}
	if iter.Last() {
		r.Limit = append([]byte{}, iter.Key()...)
	}
	err = iter.Error()
	if err != nil {
		return
	}
	sizes, err := dc.db.GetApproximateSizes([]Range{r})
	size = int(sizes.Sum())
	return
}

func (dc *stConstructor_DB) newIterator() iterator.Iterator {
	return dc.db.NewIterator(dc.ro)
}

func (dc *stConstructor_DB) customTest(h *stHarness) {}

func (dc *stConstructor_DB) close() {
	if err := dc.db.Close(); err != nil {
		dc.t.Errorf("leveldb: db close: %v", err)
	}
	if err := dc.stor.Close(); err != nil {
		dc.t.Errorf("leveldb: storage close: %v", err)
	}
	dc.db = nil
	dc.stor = nil
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
	h.test("table", &stConstructor_Table{})
	h.test("memdb", &stConstructor_MemDB{})
	h.test("merged", &stConstructor_MergedMemDB{})
	h.test("leveldb", &stConstructor_DB{})
}

func (h *stHarness) test(name string, c stConstructor) {
	ho := new(stHarnessOpt)

	err := c.init(h.t, ho)
	if err != nil {
		h.t.Error("error when initializing constructor:", err.Error())
		return
	}
	defer c.close()

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
			h.t.Error("error when adding key/value:", err)
			return
		}
	}

	var size int
	size, err = c.finish()
	if err != nil {
		h.t.Error("error when finishing constructor:", err)
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
	it.Release()

	it = c.newIterator()
	defer it.Release()
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
	defer it.Release()

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

func TestSorted_SingleBig(t *testing.T) {
	h := newStHarness(t)
	h.add("big1", strings.Repeat("1", 200000))
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
