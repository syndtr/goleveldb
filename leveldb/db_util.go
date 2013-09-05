// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// Reader is the interface that wraps basic Get and NewIterator methods.
// This interface implemented by both *DB and *Snapshot.
type Reader interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	NewIterator(ro *opt.ReadOptions) iterator.Iterator
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
	return n
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

	for _, f := range s.getFiles(storage.TypeAll) {
		keep := true
		switch f.Type() {
		case storage.TypeManifest:
			keep = !s.manifest.closed() && f.Num() >= s.manifest.file.Num()
		case storage.TypeJournal:
			if d.fjournal != nil && !d.fjournal.closed() {
				keep = f.Num() >= d.fjournal.file.Num()
			} else {
				keep = f.Num() >= d.journal.file.Num()
			}
		case storage.TypeTable:
			_, keep = tables[f.Num()]
		}

		if !keep {
			f.Remove()
		}
	}
}
