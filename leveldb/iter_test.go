// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/testutil"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type newIterator interface {
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

type iteratorInitializer interface {
	Build(t *iteratorTesting) newIterator
	Test(t *iteratorTesting, ni newIterator)
}

type tableInit struct{}

func (tableInit) Build(t *iteratorTesting) newIterator {
	buf := &bytes.Buffer{}
	o := &opt.Options{
		BlockSize:            512,
		BlockRestartInterval: 3,
	}
	tw := table.NewWriter(buf, o)
	t.Iterate(func(i int, key, value []byte) {
		tw.Append(key, value)
	})
	if err := tw.Close(); err != nil {
		t.T.Fatalf("table: err=%v", err)
	}
	t.T.Logf("table: entry_num=%d blocks=%d size=%d", tw.EntriesLen(), tw.BlocksLen(), tw.BytesLen())
	if int(tw.BytesLen()) != buf.Len() {
		t.T.Errorf("table: invalid calculated size, want %d got %d", buf.Len(), tw.BytesLen())
	}
	return table.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), nil, nil)
}

func (tableInit) Test(t *iteratorTesting, ni newIterator) {
	tr := ni.(*table.Reader)
	testfn := func(met string, findKey, key, value []byte) {
		key_, value_, err := tr.Find(findKey, nil)
		if err != nil {
			t.Errorf("table: "+met+": err=%v", err)
		}
		if !bytes.Equal(key, key_) {
			t.Errorf("table: "+met+": invalid key: want %q got %q", key, value_)
		}
		if !bytes.Equal(value, value_) {
			t.Errorf("table: "+met+": invalid value: want %q got %q", value, value_)
		}
	}
	testutil.ShuffledIndex(t.rand, t.Len(), 1, func(i int) {
		key, value := t.Index(i)
		testfn("Find", key, key, value)
	})
	testutil.ShuffledIndex(t.rand, t.Len(), 1, func(i int) {
		key_, key, value := t.IndexInexact(i)
		testfn("FindInexact", key_, key, value)
	})
}

type memdbNewIterator struct {
	*memdb.DB
}

func (ni memdbNewIterator) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	return ni.DB.NewIterator(slice)
}

type memdbInit struct{}

func (memdbInit) Build(t *iteratorTesting) newIterator {
	db := memdb.New(comparer.DefaultComparer, 0)
	t.IterateShuffled(t.rand, func(i int, key, value []byte) {
		db.Put(key, value)
	})
	return memdbNewIterator{db}
}

func (memdbInit) Test(t *iteratorTesting, ni newIterator) {}

type mergedMemdbNewIterator []*memdb.DB

func (ni mergedMemdbNewIterator) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	iters := make([]iterator.Iterator, len(ni))
	for i := range iters {
		iters[i] = ni[i].NewIterator(slice)
	}
	return iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
}

func (mergedMemdbNewIterator) Test(t *iteratorTesting, ni newIterator) {}

type mergedMemdbInit struct{}

func (mergedMemdbInit) Build(t *iteratorTesting) newIterator {
	var db [3]*memdb.DB
	for i := range db {
		db[i] = memdb.New(comparer.DefaultComparer, 0)
	}
	t.IterateShuffled(t.rand, func(i int, key, value []byte) {
		db[t.rand.Intn(len(db))].Put(key, value)
	})
	return mergedMemdbNewIterator(db[:])
}

func (mergedMemdbInit) Test(t *iteratorTesting, ni newIterator) {}

type iteratorTesting struct {
	*testing.T
	testutil.KeyValue
	rand *rand.Rand
}

func (t *iteratorTesting) test(ni newIterator, kv testutil.KeyValue, r *util.Range) {
	it := testutil.IteratorTesting{
		T:        t.T,
		KeyValue: kv,
		Iter:     ni.NewIterator(r, nil),
		Rand:     t.rand,
	}
	it.Test()
	it.Iter.Release()
}

func (t *iteratorTesting) TestFull(ni newIterator) {
	t.test(ni, t.Clone(), nil)
}

func (t *iteratorTesting) TestSlice(ni newIterator, start, limit int) {
	r := t.Range(start, limit)
	t.test(ni, t.Slice(start, limit), &r)
}

func (t *iteratorTesting) TestRange(ni newIterator, r *util.Range) {
	t.test(ni, t.SliceRange(r), r)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (t *iteratorTesting) TestOne(name string, init iteratorInitializer) {
	t.Logf("Testing %s ...", name)
	ni := init.Build(t)
	t.TestFull(ni)
	testutil.RandomIndex(t.rand, t.Len(), min(t.Len()*3, 130), func(i int) {
		key_, _, _ := t.IndexInexact(i)
		t.TestRange(ni, &util.Range{Start: key_, Limit: nil})
		t.TestRange(ni, &util.Range{Start: nil, Limit: key_})
	})
	testutil.RandomRange(t.rand, t.Len(), min(t.Len()*3, 130), func(start, limit int) {
		t.TestSlice(ni, start, limit)
	})
	init.Test(t, ni)
}

func (t *iteratorTesting) Test() {
	t.TestOne("table", tableInit{})
	t.TestOne("memdb", memdbInit{})
	t.TestOne("merged", mergedMemdbInit{})
}

func newIteratorTesting(t *testing.T) *iteratorTesting {
	seed := time.Now().UnixNano()
	t.Logf("IteratorTesting: seed %d", seed)
	return &iteratorTesting{
		T:    t,
		rand: rand.New(rand.NewSource(seed)),
	}
}

func TestIter_EmptyKey(t_ *testing.T) {
	t := newIteratorTesting(t_)
	t.PutString("", "v")
	t.Test()
}

func TestIter_EmptyValue(t_ *testing.T) {
	t := newIteratorTesting(t_)
	t.PutString("abc", "")
	t.PutString("abcd", "")
	t.Test()
}

func TestIter_Single(t_ *testing.T) {
	t := newIteratorTesting(t_)
	t.PutString("abc", "v")
	t.Test()
}

func TestIter_SingleBig(t_ *testing.T) {
	t := newIteratorTesting(t_)
	t.PutString("big1", strings.Repeat("1", 200000))
	t.Test()
}

func TestIter_Multi(t_ *testing.T) {
	t := newIteratorTesting(t_)
	t.PutString("a", "v")
	t.PutString("aa", "v1")
	t.PutString("aaa", "v2")
	t.PutString("aaacccccccccc", "v2")
	t.PutString("aaaccccccccccd", "v3")
	t.PutString("aaaccccccccccf", "v4")
	t.PutString("aaaccccccccccfg", "v5")
	t.PutString("ab", "v6")
	t.PutString("abc", "v7")
	t.PutString("abcd", "v8")
	t.PutString("accccccccccccccc", "v9")
	t.PutString("b", "v10")
	t.PutString("bb", "v11")
	t.PutString("bc", "v12")
	t.PutString("c", "v13")
	t.PutString("c1", "v13")
	t.PutString("czzzzzzzzzzzzzz", "v14")
	t.PutString("fffffffffffffff", "v15")
	t.PutString("g11", "v15")
	t.PutString("g111", "v15")
	t.PutString("g111\xff", "v15")
	t.PutString("zz", "v16")
	t.PutString("zzzzzzz", "v16")
	t.PutString("zzzzzzzzzzzzzzzz", "v16")
	t.Test()
}

func TestIter_SpecialKey(t_ *testing.T) {
	t := newIteratorTesting(t_)
	t.PutString("\xff\xff", "v3")
	t.Test()
}

func TestIter_GeneratedShort(t_ *testing.T) {
	t := newIteratorTesting(t_)
	t.PutString("", "v")
	n := 0
	for c := byte('a'); c <= byte('o'); c++ {
		for i := 1; i < 10; i++ {
			key := bytes.Repeat([]byte{c}, i)
			t.PutString(string(key), "v"+fmt.Sprint(n))
			n++
		}
	}
	t.Test()
}

func TestIter_GeneratedLong(t_ *testing.T) {
	t := newIteratorTesting(t_)
	n := 0
	for c := byte('a'); c <= byte('o'); c++ {
		for i := 150; i < 180; i++ {
			key := bytes.Repeat([]byte{c}, i)
			t.PutString(string(key), "v"+fmt.Sprint(n))
			n++
		}
	}
	t.Test()
}
