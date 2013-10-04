// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/memdb"
)

// Get latest sequence number.
func (d *DB) getSeq() uint64 {
	return atomic.LoadUint64(&d.seq)
}

// Atomically adds delta to seq.
func (d *DB) addSeq(delta uint64) {
	atomic.AddUint64(&d.seq, delta)
}

// Create new memdb and froze the old one; need external synchronization.
// newMem only called synchronously by the writer.
func (d *DB) newMem() (mem *memdb.DB, err error) {
	s := d.s

	num := s.allocFileNum()
	file := s.getJournalFile(num)
	w, err := file.Create()
	if err != nil {
		s.reuseFileNum(num)
		return
	}
	d.memMu.Lock()
	if d.journal == nil {
		d.journal = journal.NewWriter(w)
	} else {
		d.journal.Reset(w)
		d.journalWriter.Close()
		d.frozenJournalFile = d.journalFile
	}
	d.journalWriter = w
	d.journalFile = file
	d.frozenMem = d.mem
	d.mem = memdb.New(s.cmp, toPercent(d.s.o.GetWriteBuffer(), kWriteBufferPercent))
	mem = d.mem
	// The seq only incremented by the writer.
	d.frozenSeq = d.seq
	d.memMu.Unlock()
	return
}

// Get mem; no barrier.
func (d *DB) getMemNB() (mem, frozenMem *memdb.DB) {
	return d.mem, d.frozenMem
}

// Get mem.
func (d *DB) getMem() (mem, frozenMem *memdb.DB) {
	d.memMu.RLock()
	defer d.memMu.RUnlock()
	return d.mem, d.frozenMem
}

// Check whether we has frozen mem.
func (d *DB) hasFrozenMem() bool {
	d.memMu.RLock()
	defer d.memMu.RUnlock()
	return d.frozenMem != nil
}

// Get current frozen mem; assume that frozen mem isn't nil.
func (d *DB) getFrozenMem() *memdb.DB {
	d.memMu.RLock()
	defer d.memMu.RUnlock()
	return d.frozenMem
}

// Drop frozen mem; assume that frozen mem isn't nil.
func (d *DB) dropFrozenMem() {
	d.memMu.Lock()
	d.frozenJournalFile.Remove()
	d.frozenJournalFile = nil
	d.frozenMem = nil
	d.memMu.Unlock()
}

// Set closed flag; return true if not already closed.
func (d *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&d.closed, 0, 1)
}

// Check whether DB was closed.
func (d *DB) isClosed() bool {
	return atomic.LoadUint32(&d.closed) != 0
}

// Check read ok status.
func (d *DB) ok() error {
	if d.isClosed() {
		return ErrClosed
	}
	return nil
}
