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
// This interface implemented by both DB and Snapshot.
type Reader interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	NewIterator(ro *opt.ReadOptions) iterator.Iterator
}

// Range is a key range.
type Range struct {
	// Start of the key range, include in the range.
	Start []byte

	// Limit of the key range, not include in the range.
	Limit []byte
}

type Sizes []uint64

// Sum returns sum of the sizes.
func (p Sizes) Sum() (n uint64) {
	for _, s := range p {
		n += s
	}
	return n
}

// Remove unused files.
func (d *DB) cleanFiles() error {
	s := d.s

	v := s.version_NB()
	tables := make(map[uint64]struct{})
	for _, tt := range v.tables {
		for _, t := range tt {
			tables[t.file.Num()] = struct{}{}
		}
	}

	ff, err := s.getFiles(storage.TypeAll)
	if err != nil {
		return err
	}
	var rem []storage.File
	for _, f := range ff {
		keep := true
		switch f.Type() {
		case storage.TypeManifest:
			keep = f.Num() >= s.manifestFile.Num()
		case storage.TypeJournal:
			if d.frozenJournalFile != nil {
				keep = f.Num() >= d.frozenJournalFile.Num()
			} else {
				keep = f.Num() >= d.journalFile.Num()
			}
		case storage.TypeTable:
			_, keep = tables[f.Num()]
		}

		if !keep {
			rem = append(rem, f)
		}
	}
	s.logf("db@janitor F·%d G·%d", len(ff), len(rem))
	for _, f := range rem {
		s.logf("db@janitor removing %s-%d", f.Type(), f.Num())
		if err := f.Remove(); err != nil {
			return err
		}
	}
	return nil
}
