// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

func shorten(str string) string {
	if len(str) <= 13 {
		return str
	}
	return str[:5] + "..." + str[len(str)-5:]
}

func toPercent(n, percent int) int {
	return n * percent / 100
}

type files []storage.File

func (p files) Len() int {
	return len(p)
}

func (p files) Less(i, j int) bool {
	return p[i].Num() < p[j].Num()
}

func (p files) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p files) sort() {
	sort.Sort(p)
}
