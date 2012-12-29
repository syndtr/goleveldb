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

package db

import (
	"bytes"
	"testing"
)

func decodeEncode(v *sessionRecord) (res bool, err error) {
	b := new(bytes.Buffer)
	err = v.encodeTo(b)
	if err != nil {
		return
	}
	v2 := new(sessionRecord)
	err = v.decodeFrom(b)
	if err != nil {
		return
	}
	b2 := new(bytes.Buffer)
	err = v2.encodeTo(b2)
	if err != nil {
		return
	}
	return bytes.Equal(b.Bytes(), b2.Bytes()), nil
}

func TestSessionRecord_EncodeDecode(t *testing.T) {
	big := uint64(1) << 50
	v := new(sessionRecord)
	i := uint64(0)
	test := func() {
		res, err := decodeEncode(v)
		if err != nil {
			t.Fatalf("error when testing encode/decode sessionRecord: %v", err)
		}
		if !res {
			t.Error("encode/decode test failed at iteration:", i)
		}
	}

	for ; i < 4; i++ {
		test()
		v.addTable(3, big+300+i, big+400+i,
			newIKey([]byte("foo"), big+500+1, tVal),
			newIKey([]byte("zoo"), big+600+1, tDel))
		v.deleteTable(4, big+700+i)
		v.addCompactPointer(int(i), newIKey([]byte("x"), big+900+1, tVal))
	}

	v.setComparer("foo")
	v.setLogNum(big + 100)
	v.setNextNum(big + 200)
	v.setSeq(big + 1000)
	test()
}
