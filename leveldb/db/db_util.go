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
	"leveldb/log"
	"leveldb/memdb"
)

func (d *DB) newMem() (err error) {
	s := d.s

	d.mu.Lock()
	defer d.mu.Unlock()

	// create new log
	file := s.getLogFile(s.allocFileNum())
	w, err := file.Create()
	if err != nil {
		return
	}
	d.log = log.NewWriter(w)
	if d.logw != nil {
		d.logw.Close()
	}
	d.logw = w

	d.flogf = d.logf
	d.logf = file

	// new mem
	d.fmem = d.mem
	d.mem = memdb.New(s.cmp)

	d.fseq = d.seq

	return
}

func (d *DB) hasFrozenMem() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.fmem != nil
}

func (d *DB) dropFrozenMem() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fmem = nil

	d.flogf.Remove()
	d.flogf = nil
}

func (d *DB) setError(err error) {
	d.mu.Lock()
	d.err = err
	d.mu.Unlock()
}

func (d *DB) getClosed() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.closed
}

func (d *DB) ok() error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.err != nil {
		return d.err
	}
	if d.closed {
		return ErrClosed
	}
	return nil
}
