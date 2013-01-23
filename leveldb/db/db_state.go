// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
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
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"sync/atomic"
	"unsafe"
)

// Get latest sequence number.
func (d *DB) getSeq() uint64 {
	return atomic.LoadUint64(&d.seq)
}

// Atomically adds delta to seq.
func (d *DB) addSeq(delta uint64) {
	atomic.AddUint64(&d.seq, delta)
}

type memSet struct {
	cur, froze *memdb.DB
}

// Create new memdb and froze the old one; need external synchronization.
func (d *DB) newMem() (m *memdb.DB, err error) {
	s := d.s

	num := s.allocFileNum()
	w, err := newLogWriter(s.getLogFile(num))
	if err != nil {
		s.reuseFileNum(num)
		return
	}

	old := d.log
	d.log = w
	if old != nil {
		old.close()
		d.flog = old
	}

	d.fseq = d.seq

	m = memdb.New(s.cmp)
	mem := &memSet{cur: m}
	if old := d.getMem_NB(); old != nil {
		mem.froze = old.cur
	}
	atomic.StorePointer(&d.mem, unsafe.Pointer(mem))

	return
}

// Get mem; no barrier.
func (d *DB) getMem_NB() *memSet {
	return (*memSet)(d.mem)
}

// Get mem.
func (d *DB) getMem() *memSet {
	return (*memSet)(atomic.LoadPointer(&d.mem))
}

// Check whether we has frozen mem; assume that mem wasn't nil.
func (d *DB) hasFrozenMem() bool {
	if mem := d.getMem(); mem.froze != nil {
		return true
	}
	return false
}

// Get current frozen mem; assume that mem wasn't nil.
func (d *DB) getFrozenMem() *memdb.DB {
	return d.getMem().froze
}

// Drop frozen mem; assume that mem wasn't nil and frozen mem present.
func (d *DB) dropFrozenMem() {
	d.flog.remove()
	d.flog = nil
	for {
		old := d.mem
		mem := &memSet{cur: (*memSet)(old).cur}
		if atomic.CompareAndSwapPointer(&d.mem, old, unsafe.Pointer(mem)) {
			break
		}
	}
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
func (d *DB) rok() error {
	if d.isClosed() {
		return errors.ErrClosed
	}
	return nil
}

type errWrap struct {
	err error
}

// Set background error.
func (d *DB) seterr(err error) {
	if err == nil {
		atomic.StorePointer(&d.err, nil)
	} else {
		atomic.StorePointer(&d.err, unsafe.Pointer(&errWrap{err}))
	}
}

// Get background error.
func (d *DB) geterr() error {
	if p := atomic.LoadPointer(&d.err); p != nil {
		return (*errWrap)(p).err
	}
	return nil
}

// Check write ok status.
func (d *DB) wok() error {
	if err := d.geterr(); err != nil {
		return err
	}
	if d.isClosed() {
		return errors.ErrClosed
	}
	return nil
}
