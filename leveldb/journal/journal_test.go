// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package journal

import (
	"bytes"
	"math/rand"
	"testing"
)

func randomString(n int) []byte {
	b := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		b.WriteByte(' ' + byte(rand.Intn(95)))
	}
	return b.Bytes()
}

type Harness struct {
	t *testing.T

	array [][]byte
}

func NewHarness(t *testing.T) *Harness {
	return &Harness{t: t}
}

func (h *Harness) Add(v []byte) {
	h.array = append(h.array, v)
}

func (h *Harness) Test() {
	buf := new(bytes.Buffer)

	w := NewWriter(buf)
	for i, v := range h.array {
		err := w.Append(v)
		if err != nil {
			h.t.Errorf("error when adding record: '%d': %v", i, err)
		}
	}

	r := NewReader(bytes.NewReader(buf.Bytes()), true, nil)
	for i, v := range h.array {
		if !r.Next() {
			h.t.Errorf("early eof on record: '%d'", i)
			break
		}
		if r.Error() != nil {
			h.t.Errorf("error when getting record: '%d': %v", i, r.Error())
		}
		if !bytes.Equal(v, r.Record()) {
			h.t.Errorf("record '%d' is not equal, %v != %v", i, v, r.Record())
		}
	}

	if r.Next() {
		h.t.Error("expectiong eof")
	}
	if r.Record() != nil {
		h.t.Error("record should be nil")
	}
}

func TestJournalSimpleRandomShort(t *testing.T) {
	h := NewHarness(t)
	for i := 0; i < 100; i++ {
		v := randomString(rand.Intn(10))
		h.Add(v)
	}
	h.Test()
}

func TestJournalSimpleRandomLong(t *testing.T) {
	h := NewHarness(t)
	for i := 0; i < 20; i++ {
		v := randomString(rand.Intn(BlockSize * 3))
		h.Add(v)
	}
	h.Test()
}
