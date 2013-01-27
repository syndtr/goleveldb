// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package desc

import (
	"bytes"
	"testing"
)

func TestMemDesc(t *testing.T) {
	m := new(MemDesc)

	f := m.GetFile(1, TypeTable)
	if f.Num() != 1 && f.Type() != TypeTable {
		t.Fatal("invalid file number and type")
	}
	w, _ := f.Create()
	w.Write([]byte("abc"))
	w.Close()
	if len(m.GetFiles(TypeAll)) != 1 {
		t.Fatal("invalid GetFiles len")
	}
	buf := new(bytes.Buffer)
	r, err := f.Open()
	if err != nil {
		t.Fatal("Open: got error: ", err)
	}
	buf.ReadFrom(r)
	r.Close()
	if got := buf.String(); got != "abc" {
		t.Fatalf("Read: invalid value, want=abc got=%s", got)
	}
	f.Rename(2, TypeLog)
	if f.Num() != 2 && f.Type() != TypeLog {
		t.Fatal("invalid file number and type")
	}
	if _, err := f.Open(); err != nil {
		t.Fatal("Open: got error: ", err)
	}
	if ff := m.GetFiles(TypeAll); len(ff) != 1 {
		t.Fatal("invalid GetFiles len")
	} else {
		if ff[0].Num() != 2 && ff[0].Type() != TypeLog {
			t.Fatal("invalid file number and type")
		}
	}
	if _, err := m.GetFile(1, TypeTable).Open(); err == nil {
		t.Fatal("expecting error")
	}
	f.Remove()
	if len(m.GetFiles(TypeAll)) != 0 {
		t.Fatal("invalid GetFiles len", m.GetFiles(TypeAll))
	}
	if _, err := f.Open(); err == nil {
		t.Fatal("expecting error")
	}
}
