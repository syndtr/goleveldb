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
	"encoding/binary"
	"leveldb"
)

type iKeyComparator struct {
	cmp leveldb.Comparator
}

func (p *iKeyComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

func (p *iKeyComparator) Compare(a, b []byte) int {
	r := p.cmp.Compare(iKey(a).ukey(), iKey(b).ukey())
	if r == 0 {
		var an, bn uint64
		binary.Read(bytes.NewBuffer(a[len(a)-8:]), binary.LittleEndian, &an)
		binary.Read(bytes.NewBuffer(b[len(b)-8:]), binary.LittleEndian, &bn)
		if an > bn {
			r = -1
		} else if an < bn {
			r = 1
		}
	}
	return r
}

func (p *iKeyComparator) FindShortestSeparator(a, b []byte) []byte {
	ua, ub := iKey(a).ukey(), iKey(b).ukey()
	r := p.cmp.FindShortestSeparator(ua, ub)
	if len(r) < len(ua) && p.cmp.Compare(ua, r) < 0 {
		buf := new(bytes.Buffer)
		buf.Write(r)
		binary.Write(buf, binary.LittleEndian,
			packSequenceAndType(kMaxSeq, tSeek))
		return buf.Bytes()
	}
	return append(r, a[len(r):]...)
}

func (p *iKeyComparator) FindShortSuccessor(b []byte) []byte {
	ub := iKey(b).ukey()
	r := p.cmp.FindShortSuccessor(ub)
	if len(r) < len(ub) && p.cmp.Compare(ub, r) < 0 {
		buf := new(bytes.Buffer)
		buf.Write(r)
		binary.Write(buf, binary.LittleEndian,
			packSequenceAndType(kMaxSeq, tSeek))
		return buf.Bytes()
	}
	return append(r, b[len(r):]...)
}
