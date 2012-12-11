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
	"encoding/binary"
	"testing"
)

type Harness struct {
	t *testing.T

	bloom  *BloomFilter
	filter []byte
	keys   [][]byte
}

func NewHarness(t *testing.T) *Harness {
	return &Harness{t: t, bloom: NewBloomFilter(10)}
}

func (h *Harness) Add(key []byte) {
	h.keys = append(h.keys, key)
}

func (h *Harness) AddNum(key uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, key)
	h.Add(buf.Bytes())
}

func (h *Harness) Build() {
	buf := new(bytes.Buffer)
	h.bloom.CreateFilter(h.keys, buf)
	h.filter = buf.Bytes()
}

func (h *Harness) Reset() {
	h.filter = nil
	h.keys = nil
}

func (h *Harness) FilterLen() int {
	return len(h.filter)
}

func (h *Harness) Assert(key []byte, want, silent bool) bool {
	got := h.bloom.KeyMayMatch(key, h.filter)
	if !silent && got != want {
		h.t.Errorf("Assert on '%v' failed got '%v', want '%v'", key, got, want)
	}
	return got
}

func (h *Harness) AssertNum(key uint32, want, silent bool) bool {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, key)
	return h.Assert(buf.Bytes(), want, silent)
}

func TestBloomFilter_Empty(t *testing.T) {
	h := NewHarness(t)
	h.Build()
	h.Assert([]byte("hello"), false, false)
	h.Assert([]byte("world"), false, false)
}

func TestBloomFilter_Small(t *testing.T) {
	h := NewHarness(t)
	h.Add([]byte("hello"))
	h.Add([]byte("world"))
	h.Build()
	h.Assert([]byte("hello"), true, false)
	h.Assert([]byte("world"), true, false)
	h.Assert([]byte("x"), false, false)
	h.Assert([]byte("foo"), false, false)
}

func nextN(n int) int {
	switch {
	case n < 10:
		n += 1
	case n < 100:
		n += 10
	case n < 1000:
		n += 100
	default:
		n += 1000
	}
	return n
}

func TestBloomFilter_VaryingLengths(t *testing.T) {
	h := NewHarness(t)
	var mediocre, good int
	for n := 1; n < 10000; n = nextN(n) {
		h.Reset()
		for i := 0; i < n; i++ {
			h.AddNum(uint32(i))
		}
		h.Build()

		got := h.FilterLen()
		want := (n * 10 / 8) + 40
		if got > want {
			t.Errorf("filter len test failed, '%d' > '%d'", got, want)
		}

		for i := 0; i < n; i++ {
			h.AssertNum(uint32(i), true, false)
		}

		var rate float32
		for i := 0; i < 10000; i++ {
			if h.AssertNum(uint32(i+1000000000), true, true) {
				rate++
			}
		}
		rate /= 10000
		if rate > 0.02 {
			t.Errorf("false positive rate is more than 2%%, got %v, at len %d", rate, n)
		}
		if rate > 0.0125 {
			mediocre++
		} else {
			good++
		}
	}
	t.Logf("false positive rate: %d good, %d mediocre", good, mediocre)
	if mediocre > good/5 {
		t.Error("mediocre false positive rate is more than expected")
	}
}
