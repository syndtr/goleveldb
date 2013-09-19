// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"runtime"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var errSnapshotReleased = errors.New("leveldb: snapshot released")

type snapshotElement struct {
	seq uint64
	ref int
	// Next and previous pointers in the doubly-linked list of elements.
	next, prev *snapshotElement
}

// Initialize the snapshot.
func (db *DB) initSnapshot() {
	db.snapsRoot.next = &db.snapsRoot
	db.snapsRoot.prev = &db.snapsRoot
}

// Acquires a snapshot, based on latest sequence.
func (db *DB) acquireSnapshot() *snapshotElement {
	db.snapsMu.Lock()
	seq := db.getSeq()
	elem := db.snapsRoot.prev
	if elem == &db.snapsRoot || elem.seq != seq {
		at := db.snapsRoot.prev
		next := at.next
		elem = &snapshotElement{
			seq:  seq,
			prev: at,
			next: next,
		}
		at.next = elem
		next.prev = elem
	}
	elem.ref++
	db.snapsMu.Unlock()
	return elem
}

// Releases given snapshot element.
func (db *DB) releaseSnapshot(elem *snapshotElement) {
	db.snapsMu.Lock()
	elem.ref--
	if elem.ref == 0 {
		elem.prev.next = elem.next
		elem.next.prev = elem.prev
		elem.next = nil
		elem.prev = nil
	} else if elem.ref < 0 {
		panic("leveldb: Snapshot: negative element reference")
	}
	db.snapsMu.Unlock()
}

// Gets minimum sequence that not being snapshoted.
func (db *DB) minSeq() uint64 {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()
	elem := db.snapsRoot.prev
	if elem != &db.snapsRoot {
		return elem.seq
	}
	return db.getSeq()
}

// Snapshot represent a DB snapshot.
type Snapshot struct {
	db       *DB
	elem     *snapshotElement
	mu       sync.Mutex
	released bool
}

// Creates new snapshot object.
func (db *DB) newSnapshot() *Snapshot {
	p := &Snapshot{
		db:   db,
		elem: db.acquireSnapshot(),
	}
	runtime.SetFinalizer(p, (*Snapshot).Release)
	return p
}

// Get gets the value for the given key. It returns errors.ErrNotFound if
// the DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (p *Snapshot) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	db := p.db
	err = db.rok()
	if err != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		err = errSnapshotReleased
		return
	}
	return db.get(key, p.elem.seq, ro)
}

// NewIterator returns an iterator for the snapshot of the uderlying DB.
//
// The returned iterator is not goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
//
// It is safe to use an iterator concurrently with modifying its
// underlying DB. The resultant key/value pairs are guaranteed to be
// consistent.
//
// Releasing the snapshot doesn't mean releasing the iterator too, the
// iterator would be still valid until explicitly released.
func (p *Snapshot) NewIterator(ro *opt.ReadOptions) iterator.Iterator {
	db := p.db
	if err := db.rok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return iterator.NewEmptyIterator(errSnapshotReleased)
	}
	return db.newIterator(p.elem.seq, ro)
}

// Release releases the snapshot. The caller must not use the snapshot
// after this call.
func (p *Snapshot) Release() {
	p.mu.Lock()
	if !p.released {
		// not needed anymore
		runtime.SetFinalizer(p, nil)

		p.released = true
		p.db.releaseSnapshot(p.elem)
		p.db = nil
		p.elem = nil
	}
	p.mu.Unlock()
}
