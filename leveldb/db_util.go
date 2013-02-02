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
	"leveldb/descriptor"
	"leveldb/iter"
	"leveldb/opt"
)

// Reader is the interface that wraps basic Get and NewIterator methods.
// This interface implemented by both *DB and *Snapshot.
type Reader interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	NewIterator(ro *opt.ReadOptions) iter.Iterator
}

// Range represent key range.
type Range struct {
	// Start key, include in the range
	Start []byte

	// Limit, not include in the range
	Limit []byte
}

type Sizes []uint64

// Sum return sum of the sizes.
func (p Sizes) Sum() (n uint64) {
	for _, s := range p {
		n += s
	}
	return
}

// Remove unused files.
func (d *DB) cleanFiles() {
	s := d.s

	v := s.version()
	tables := make(map[uint64]struct{})
	for _, tt := range v.tables {
		for _, t := range tt {
			tables[t.file.Num()] = struct{}{}
		}
	}

	for _, f := range s.getFiles(descriptor.TypeAll) {
		keep := true
		switch f.Type() {
		case descriptor.TypeManifest:
			keep = !s.manifest.closed() && f.Num() >= s.manifest.file.Num()
		case descriptor.TypeLog:
			if d.flog != nil && !d.flog.closed() {
				keep = f.Num() >= d.flog.file.Num()
			} else {
				keep = f.Num() >= d.log.file.Num()
			}
		case descriptor.TypeTable:
			_, keep = tables[f.Num()]
		}

		if !keep {
			f.Remove()
		}
	}
}
