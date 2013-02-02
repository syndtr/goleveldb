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

package descriptor

import (
	"fmt"
	"os"
	"path"
	"testing"
)

var cases = []struct {
	name  string
	ftype FileType
	num   uint64
}{
	{"100.log", TypeLog, 100},
	{"0.log", TypeLog, 0},
	{"0.sst", TypeTable, 0},
	{"0.dbtmp", TypeTemp, 0},
	{"MANIFEST-2", TypeManifest, 2},
	{"MANIFEST-7", TypeManifest, 7},
	{"18446744073709551615.log", TypeLog, 18446744073709551615},
}

var invalidCases = []string{
	"",
	"foo",
	"foo-dx-100.log",
	".log",
	"",
	"manifest",
	"CURREN",
	"CURRENTX",
	"MANIFES",
	"MANIFEST",
	"MANIFEST-",
	"XMANIFEST-3",
	"MANIFEST-3x",
	"LOC",
	"LOCKx",
	"LO",
	"LOGx",
	"18446744073709551616.log",
	"184467440737095516150.log",
	"100",
	"100.",
	"100.lop",
}

func TestFileDesc_CreateFileName(t *testing.T) {
	for _, c := range cases {
		f := &file{num: c.num, t: c.ftype}
		if f.name() != c.name {
			t.Errorf("invalid filename got '%s', want '%s'", f.name(), c.name)
		}
	}
}

func TestFileDesc_ParseFileName(t *testing.T) {
	for _, c := range cases {
		f := new(file)
		if !f.parse(c.name) {
			t.Errorf("cannot parse filename '%s'", c.name)
			continue
		}
		if f.Type() != c.ftype {
			t.Errorf("filename '%s' invalid type got '%d', want '%d'", c.name, f.Type(), c.ftype)
		}
		if f.Num() != c.num {
			t.Errorf("filename '%s' invalid number got '%d', want '%d'", c.name, f.Num(), c.num)
		}
	}
}

func TestFileDesc_InvalidFileName(t *testing.T) {
	for _, name := range invalidCases {
		f := new(file)
		if f.parse(name) {
			t.Errorf("filename '%s' should be invalid", name)
		}
	}
}

func TestFileDesc_Flock(t *testing.T) {
	pth := path.Join(os.TempDir(), fmt.Sprintf("goleveldbtestfd-%d", os.Getuid()))

	_, err := os.Stat(pth)
	if err == nil {
		err = os.RemoveAll(pth)
		if err != nil {
			t.Fatal("RemoveAll: got error: ", err)
		}
	}

	p1, err := OpenFile(pth)
	if err != nil {
		t.Fatal("OpenFile(1): got error: ", err)
	}

	defer os.RemoveAll(pth)

	p2, err := OpenFile(pth)
	if err != nil {
		t.Log("OpenFile(2): got error: ", err)
	} else {
		p2.Close()
		p1.Close()
		t.Fatal("OpenFile(2): expect error")
	}

	p1.Close()

	p3, err := OpenFile(pth)
	if err != nil {
		t.Fatal("OpenFile(3): got error: ", err)
	}
	p3.Close()
}
