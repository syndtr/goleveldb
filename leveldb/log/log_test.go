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

package log

import (
	"bytes"
	"math/rand"
	"os"
	"testing"
	"time"
)

func randomString(n int) []byte {
	b := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		b.WriteByte(' ' + byte(rand.Intn(95)))
	}
	return b.Bytes()
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
	return r.r.Seek(offset, whence)
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

	w := NewWriter(&writer{buf})
	for i, v := range h.array {
		err := w.Append(v)
		if err != nil {
			h.t.Errorf("error when adding record: '%d': %v", i, err)
		}
	}

	rr := &reader{
		r:    bytes.NewReader(buf.Bytes()),
		name: "log",
		size: int64(len(buf.Bytes())),
	}
	r := NewReader(rr, true)
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

func TestLogSimpleRandomShort(t *testing.T) {
	h := NewHarness(t)
	for i := 0; i < 100; i++ {
		v := randomString(rand.Intn(10))
		h.Add(v)
	}
	h.Test()
}

func TestLogSimpleRandomLong(t *testing.T) {
	h := NewHarness(t)
	for i := 0; i < 20; i++ {
		v := randomString(rand.Intn(kBlockSize * 3))
		h.Add(v)
	}
	h.Test()
}
