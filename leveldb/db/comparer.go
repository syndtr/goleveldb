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

import "github.com/syndtr/goleveldb/leveldb/comparer"

type iComparer struct {
	cmp comparer.Comparer
}

func (p *iComparer) Name() string {
	return p.cmp.Name()
}

func (p *iComparer) Compare(a, b []byte) int {
	ia, ib := iKey(a), iKey(b)
	r := p.cmp.Compare(ia.ukey(), ib.ukey())
	if r == 0 {
		an, bn := ia.num(), ib.num()
		if an > bn {
			r = -1
		} else if an < bn {
			r = 1
		}
	}
	return r
}

func (p *iComparer) Separator(a, b []byte) []byte {
	ua, ub := iKey(a).ukey(), iKey(b).ukey()
	r := p.cmp.Separator(ua, ub)
	if len(r) < len(ua) && p.cmp.Compare(ua, r) < 0 {
		rr := make([]byte, len(r)+8)
		copy(rr, r)
		copy(rr[len(r):], kMaxNumBytes)
		return rr
	}
	return append(r, a[len(r):]...)
}

func (p *iComparer) Successor(b []byte) []byte {
	ub := iKey(b).ukey()
	r := p.cmp.Successor(ub)
	if len(r) < len(ub) && p.cmp.Compare(ub, r) < 0 {
		rr := make([]byte, len(r)+8)
		copy(rr, r)
		copy(rr[len(r):], kMaxNumBytes)
		return rr
	}
	return append(r, b[len(r):]...)
}
