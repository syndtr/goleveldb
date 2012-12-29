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
	"container/list"
	"leveldb"
	"leveldb/memdb"
	"sync/atomic"
)

var ErrSnapshotReleased = leveldb.ErrInvalid("snapshot released")

type snapEntry struct {
	elem *list.Element
	seq  uint64
	ref  int
}

func (d *DB) acquireSnapshot() (p *snapEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	back := d.snapshots.Back()
	if back != nil {
		p = back.Value.(*snapEntry)
	}
	num := d.seq
	if p == nil || p.seq != num {
		p = &snapEntry{seq: num}
		p.elem = d.snapshots.PushBack(p)
	}
	p.ref++
	return
}

func (d *DB) releaseSnapshot(p *snapEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p.ref--
	if p.ref == 0 {
		d.snapshots.Remove(p.elem)
	}
}

func (d *DB) minSnapshot() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	back := d.snapshots.Front()
	if back == nil {
		return d.s.seq()
	}
	return back.Value.(*snapEntry).seq
}

// Snapshot represent a database snapshot.
type Snapshot struct {
	d        *DB
	entry    *snapEntry
	released uint32
}

func (d *DB) newSnapshot() *Snapshot {
	return &Snapshot{d: d, entry: d.acquireSnapshot()}
}

// Get get value for given key of this snapshot of database.
func (p *Snapshot) Get(key []byte, ro *leveldb.ReadOptions) (value []byte, err error) {
	if atomic.LoadUint32(&p.released) != 0 {
		return nil, ErrSnapshotReleased
	}

	d := p.d
	s := d.s

	if d.getClosed() {
		return nil, ErrClosed
	}

	ucmp := s.cmp.cmp
	ikey := newIKey(key, p.entry.seq, tSeek)
	memGet := func(m *memdb.DB) bool {
		var k []byte
		k, value, err = m.Get(ikey)
		if err != nil {
			return false
		}
		ik := iKey(k)
		if ucmp.Compare(ik.ukey(), key) != 0 {
			return false
		}
		valid, _, vt := ik.seqAndType()
		if !valid {
			panic("got invalid ikey")
		}
		if vt == tDel {
			value = nil
			err = leveldb.ErrNotFound
		}
		return true
	}

	d.mu.RLock()
	if memGet(d.mem) || (d.fmem != nil && memGet(d.fmem)) {
		d.mu.RUnlock()
		return
	}
	d.mu.RUnlock()

	var cState bool
	value, cState, err = s.version().get(ikey, ro)

	if cState && !d.getClosed() {
		// schedule compaction
		select {
		case d.cch <- cSched:
		default:
		}
	}

	return
}

// NewIterator return an iterator over the contents of this snapshot of
// database. The result of NewIterator() is initially invalid (caller must
// call Next or one of Seek method, ie First, Last or Seek).
func (p *Snapshot) NewIterator(ro *leveldb.ReadOptions) leveldb.Iterator {
	if atomic.LoadUint32(&p.released) != 0 {
		return &leveldb.EmptyIterator{ErrSnapshotReleased}
	}

	d := p.d
	s := d.s

	if d.getClosed() {
		return &leveldb.EmptyIterator{ErrClosed}
	}

	return newDBIter(p.entry.seq, d.newRawIterator(ro), s.cmp.cmp)
}

// Release release the snapshot. The caller must not use the snapshot
// after this call.
func (p *Snapshot) Release() {
	if atomic.CompareAndSwapUint32(&p.released, 0, 1) {
		p.d.releaseSnapshot(p.entry)
		p.d = nil
		p.entry = nil
	}
}
