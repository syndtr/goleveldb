// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
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
		t.Errorf("offset %v not in range, want %v - %v", v, low, hi)
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
		t.Fatal("error when finalizing table:", err.Error())
	}
	size := w.Len()
	r := &reader{*bytes.NewReader(w.Bytes())}
	tr := NewReader(r, int64(size), nil, o)
	// if err != nil {
	// 	t.Fatal("error when creating table reader instance:", err.Error())
	// }

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

type blockHarness struct {
	t               *testing.T
	restartInterval int
	kv              [][]byte
}

func (h *blockHarness) append(key, value string) {
	h.kv = append(h.kv, []byte(key), []byte(value))
}

func (h *blockHarness) testKV(act string, iter iterator.Iterator, i int) {
	key, value := h.kv[i], h.kv[i+1]
	if !bytes.Equal(iter.Key(), key) {
		h.t.Fatalf("block: %s: key invalid, want=%q got=%q", act, key, iter.Key())
	}
	if !bytes.Equal(iter.Value(), value) {
		h.t.Fatalf("block: %s: value invalid, want=%q got=%q", act, value, iter.Value())
	}
}

func (h *blockHarness) prev(pref string, iter iterator.Iterator) {
	if !iter.Prev() {
		h.t.Fatalf("block: %sPrev: eoi, err=%v", pref, iter.Error())
	}
}

func (h *blockHarness) next(pref string, iter iterator.Iterator) {
	if !iter.Next() {
		h.t.Fatalf("block: %sNext: eoi, err=%v", pref, iter.Error())
	}
}

func (h *blockHarness) soi(pref string, iter iterator.Iterator) {
	if iter.Prev() {
		h.t.Fatalf("block: %sPrev: expect soi", pref)
	} else if err := iter.Error(); err != nil {
		h.t.Fatalf("block: %sPrev: expect soi but got error, err=%v", pref, err)
	}
}

func (h *blockHarness) eoi(pref string, iter iterator.Iterator) {
	if iter.Next() {
		h.t.Fatalf("block: %sNext: expect eoi", pref)
	} else if err := iter.Error(); err != nil {
		h.t.Fatalf("block: %sNext: expect eoi but got error, err=%v", pref, err)
	}
}

func (h *blockHarness) first(iter iterator.Iterator) {
	if len(h.kv) == 0 {
		if iter.First() {
			h.t.Fatalf("block: First: expect soi, key=%q", iter.Key())
		}
	} else {
		if !iter.First() {
			h.t.Fatalf("block: First: soi, err=%v", iter.Error())
		}
	}
}

func (h *blockHarness) last(iter iterator.Iterator) {
	if len(h.kv) == 0 {
		if iter.Last() {
			h.t.Fatalf("block: Last: expect eoi, key=%q", iter.Key())
		}
	} else {
		if !iter.Last() {
			h.t.Fatalf("block: Last: eoi, err=%v", iter.Error())
		}
	}
}

func (h *blockHarness) test() {
	h.t.Logf("block: entry_num=%d", len(h.kv)/2)
	w := &blockWriter{
		restartInterval: h.restartInterval,
		scratch:         make([]byte, 30),
	}
	for i := 0; i < len(h.kv); i += 2 {
		w.append(h.kv[i], h.kv[i+1])
	}
	w.finish()
	data := w.buf.Bytes()
	restartsLen := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	r := &block{
		cmp:            comparer.DefaultComparer,
		data:           data,
		restartsLen:    restartsLen,
		restartsOffset: len(data) - (restartsLen+1)*4,
	}
	iter := r.newIterator(nil)
	defer iter.Release()
	for m := 0; m < 3; m++ {
		h.t.Logf("block: retry=%d", m)
		for i := 0; i < len(h.kv); i += 2 {
			h.next("", iter)
			h.testKV("Next", iter, i)
		}
		h.eoi("", iter)
		for i := len(h.kv) - 2; i >= 0; i -= 2 {
			h.prev("", iter)
			h.testKV("Prev", iter, i)
		}
		h.soi("", iter)
	}
	h.last(iter)
	if len(h.kv) > 0 {
		h.testKV("Last", iter, len(h.kv)-2)
		for i := len(h.kv) - 4; i >= 0; i -= 2 {
			h.prev("Last: ", iter)
			h.testKV("Last: Prev", iter, i)
		}
		h.last(iter)
	} else {
		h.soi("Last: ", iter)
		h.eoi("Last: ", iter)
		h.last(iter)
		h.eoi("Last2: ", iter)
		h.soi("Last2: ", iter)
	}
	h.first(iter)
	if len(h.kv) > 0 {
		h.testKV("First", iter, 0)
		for i := 2; i < len(h.kv); i += 2 {
			h.next("Last: ", iter)
			h.testKV("First: Next", iter, i)
		}
	} else {
		h.eoi("First: ", iter)
		h.soi("First: ", iter)
		h.first(iter)
		h.soi("First2: ", iter)
		h.eoi("First2: ", iter)
	}
	for i := 0; i < len(h.kv); i += 2 {
		for x := 0; x < 2; x++ {
			if !iter.Seek(h.kv[i]) {
				h.t.Fatalf("block: Seek: eoi, err=%v", iter.Error())
			}
			h.testKV("Seek", iter, i)
			if x == 0 {
				for j := i + 2; j < len(h.kv); j += 2 {
					h.next("Seek: ", iter)
					h.testKV("Seek: Next", iter, j)
				}
				h.eoi("Seek: ", iter)
			} else {
				for j := i - 2; j >= 0; j -= 2 {
					h.prev("Seek: ", iter)
					h.testKV("Seek: Prev", iter, j)
				}
				h.soi("Seek: ", iter)
			}
		}
	}
	if len(h.kv) == 0 {
		for i := 0; i < 2; i++ {
			if iter.Seek(nil) {
				h.t.Fatal("block: SeekZero: expect eoi")
			}
			if i == 0 {
				h.eoi("SeekZero: ", iter)
				h.soi("SeekZero: ", iter)
			} else {
				h.soi("SeekZero: ", iter)
				h.eoi("SeekZero: ", iter)
			}
		}
	}
}

func TestBlockReadWrite(t *testing.T) {
	for x := 1; x <= 5; x++ {
		t.Logf("restart_interval=%d", x)
		h := blockHarness{t: t, restartInterval: x}
		h.test()
		h.append("", "empty")
		h.test()
		h.append("k1", "foo")
		h.test()
		h.append("k2", "v")
		h.append("k3", "hallo")
		h.test()
		h.append("k4", "bar")
		h.test()
		h.append("k5", "v5")
		h.append("k6", "")
		h.test()
		h.append("k7", "v7")
		h.append("k8", "vvvvvvvvvvvvvvvvvvvvvv8")
		h.test()
		h.append("k9", "v9")
		h.test()
	}
}
