// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"io"

	"github.com/syndtr/goleveldb/leveldb/filter"
)

type iFilter struct {
	filter filter.Filter
}

func (p *iFilter) Name() string {
	return p.filter.Name()
}

func (p *iFilter) CreateFilter(keys [][]byte, buf io.Writer) {
	nkeys := make([][]byte, len(keys))
	for i := range keys {
		nkeys[i] = iKey(keys[i]).ukey()
	}
	p.filter.CreateFilter(nkeys, buf)
}

func (p *iFilter) KeyMayMatch(key, filter []byte) bool {
	return p.filter.KeyMayMatch(iKey(key).ukey(), filter)
}
