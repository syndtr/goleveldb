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

// Package db provide implementation of LevelDB database.
package db

import (
	"container/list"
	"leveldb/descriptor"
	"leveldb/errors"
	"leveldb/iter"
	"leveldb/log"
	"leveldb/memdb"
	"leveldb/opt"
	"os"
	"runtime"
	"sync"
	"time"
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

// DB represent a database session.
type DB struct {
	s *session

	cch  chan cSignal   // compaction worker signal
	creq chan *cReq     // compaction request
	wch  chan *Batch    // write channel
	ewg  sync.WaitGroup // exit WaitGroup

	mu          sync.RWMutex
	mem, fmem   *memdb.DB
	log         *log.Writer
	logw        descriptor.Writer
	logf, flogf descriptor.File
	seq, fseq   uint64
	snapshots   list.List
	err         error
	closed      bool
}

// Open open or create database from given desc.
func Open(desc descriptor.Descriptor, o *opt.Options) (d *DB, err error) {
	s := newSession(desc, o)

	err = s.recover()
	if os.IsNotExist(err) && o.HasFlag(opt.OFCreateIfMissing) {
		err = s.create()
	} else if err == nil && o.HasFlag(opt.OFErrorIfExist) {
		err = os.ErrExist
	}
	if err != nil {
		return
	}

	d = &DB{
		s:    s,
		cch:  make(chan cSignal),
		creq: make(chan *cReq),
		wch:  make(chan *Batch),
		seq:  s.st.seq,
	}
	d.snapshots.Init()

	err = d.recoverLog()
	if err != nil {
		return
	}

	// remove any obsolete files
	d.cleanFiles()

	go d.compaction()
	go d.write()
	return
}

func (d *DB) recoverLog() (err error) {
	s := d.s
	icmp := s.cmp

	mb := new(memBatch)
	cm := newCMem(s)

	s.printf("LogRecovery: started, min=%d", s.st.logNum)

	var flogf descriptor.File
	ff := files(s.getFiles(descriptor.TypeLog))
	ff.sort()

	skip := 0
	for _, file := range ff {
		if file.Number() < s.st.logNum {
			skip++
			continue
		}
		s.markFileNum(file.Number())
	}

	ff = ff[skip:]
	for _, file := range ff {
		s.printf("LogRecovery: recovering, num=%d", file.Number())

		var r descriptor.Reader
		r, err = file.Open()
		if err != nil {
			return
		}

		if mb.mem != nil {
			d.fseq = d.seq

			if mb.mem.Len() > 0 {
				err = cm.flush(mb.mem, 0)
				if err != nil {
					return
				}
			}

			err = cm.commit(file.Number(), d.fseq)
			if err != nil {
				return
			}

			cm.reset()

			flogf.Remove()
			flogf = nil
		}

		mb.mem = memdb.New(icmp)

		lr := log.NewReader(r, true)
		for lr.Next() {
			d.seq, err = replayBatch(lr.Record(), mb)
			if err != nil {
				return
			}

			if mb.mem.Size() > s.o.GetWriteBuffer() {
				// flush to table
				err = cm.flush(mb.mem, 0)
				if err != nil {
					return
				}

				// create new memdb
				mb.mem = memdb.New(icmp)
			}
		}

		err = lr.Error()
		if err != nil {
			return
		}

		r.Close()
		flogf = file
	}

	// create new log
	err = d.newMem()
	if err != nil {
		return
	}

	if mb.mem != nil && mb.mem.Len() > 0 {
		err = cm.flush(mb.mem, 0)
		if err != nil {
			return
		}
	}

	d.fseq = d.seq

	err = cm.commit(d.logf.Number(), d.fseq)
	if err != nil {
		return
	}

	if flogf != nil {
		flogf.Remove()
	}

	return
}

func (d *DB) flush() (err error) {
	s := d.s

	delayed := false
	for {
		v := s.version()
		switch {
		case v.tLen(0) >= kL0_SlowdownWritesTrigger && !delayed:
			delayed = true
			time.Sleep(1000 * time.Microsecond)
			continue
		case d.mem.Size() <= s.o.GetWriteBuffer():
			// still room
			return
		case d.hasFrozenMem():
			d.cch <- cWait
			continue
		case v.tLen(0) >= kL0_StopWritesTrigger:
			d.cch <- cSched
			continue
		}

		// create new memdb and log
		err = d.newMem()
		if err != nil {
			return
		}

		// schedule compaction
		select {
		case d.cch <- cSched:
		default:
		}
	}

	return
}

func (d *DB) write() {
	// register to the WaitGroup
	d.ewg.Add(1)
	defer d.ewg.Done()

	lch := make(chan *Batch)
	lack := make(chan error)
	go func() {
		for b := range lch {
			if b == nil {
				close(lch)
				close(lack)
				return
			}

			// write log
			err := d.log.Append(b.encode())
			if err == nil && b.sync {
				err = d.logw.Sync()
			}

			lack <- err
		}
	}()

	for {
		b := <-d.wch
		if b == nil && d.getClosed() {
			lch <- nil
			close(d.wch)
			return
		}

		err := d.flush()
		if err != nil {
			b.done(err)
			continue
		}

		// calculate max size of batch
		n := b.size()
		m := 1 << 20
		if n <= 128<<10 {
			m = n + (128 << 10)
		}

		// merge with other batch
		for done := false; !done && b.size() <= m && !b.sync; {
			select {
			case nb := <-d.wch:
				b.append(nb)
			default:
				done = true
			}
		}

		// set batch first seq number relative from last seq
		seq := d.seq
		b.seq = seq + 1

		// write log
		lch <- b

		// replay batch to memdb
		b.memReplay(d.mem)

		// wait for log
		err = <-lack
		if err != nil {
			b.revertMemReplay(d.mem)
			b.done(err)
			continue
		}

		// set last seq number
		d.mu.Lock()
		d.seq = seq + uint64(b.len())
		d.mu.Unlock()

		// done
		b.done(nil)
	}
}

// Put set the database entry for "key" to "value".
func (d *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	b := new(Batch)
	b.Put(key, value)
	return d.Write(b, wo)
}

// Delete remove the database entry (if any) for "key". It is not an error
// if "key" did not exist in the database.
func (d *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	b := new(Batch)
	b.Delete(key)
	return d.Write(b, wo)
}

// Write apply the specified batch to the database.
func (d *DB) Write(b *Batch, wo *opt.WriteOptions) (err error) {
	err = d.ok()
	if err != nil || b == nil || b.len() == 0 {
		return
	}

	rch := b.init(wo.HasFlag(opt.WFSync))
	d.wch <- b
	err = <-rch
	close(rch)
	return
}

// Get get value for given key of the latest snapshot of database.
func (d *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	p := d.newSnapshot()
	defer p.Release()
	return p.Get(key, ro)
}

// newRawIterator return merged interators of current version, current frozen memdb
// and current memdb.
func (d *DB) newRawIterator(ro *opt.ReadOptions) iter.Iterator {
	s := d.s

	d.mu.RLock()
	ti := s.version().getIterators(ro)
	ii := make([]iter.Iterator, 0, len(ti)+2)
	ii = append(ii, d.mem.NewIterator())
	if d.fmem != nil {
		ii = append(ii, d.fmem.NewIterator())
	}
	ii = append(ii, ti...)
	d.mu.RUnlock()

	return iter.NewMergedIterator(ii, s.cmp)
}

// NewIterator return an iterator over the contents of the latest snapshot of
// database. The result of NewIterator() is initially invalid (caller must
// call Next or one of Seek method, ie First, Last or Seek).
func (d *DB) NewIterator(ro *opt.ReadOptions) iter.Iterator {
	p := d.newSnapshot()
	i := p.NewIterator(ro)
	x, ok := i.(*Iterator)
	if ok {
		runtime.SetFinalizer(x, func(x *Iterator) {
			p.Release()
		})
	} else {
		p.Release()
	}
	return i
}

// GetSnapshot return a handle to the current DB state.
// Iterators created with this handle will all observe a stable snapshot
// of the current DB state. The caller must call *Snapshot.Release() when the
// snapshot is no longer needed.
func (d *DB) GetSnapshot() (snapshot *Snapshot, err error) {
	if d.getClosed() {
		return nil, errors.ErrClosed
	}
	snapshot = d.newSnapshot()
	runtime.SetFinalizer(snapshot, func(x *Snapshot) {
		x.Release()
	})
	return
}

// GetProperty used to query exported database state.
//
// Valid property names include:
//
//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
//     where <N> is an ASCII representation of a level number (e.g. "0").
//  "leveldb.stats" - returns a multi-line string that describes statistics
//     about the internal operation of the DB.
//  "leveldb.sstables" - returns a multi-line string that describes all
//     of the sstables that make up the db contents.
func (d *DB) GetProperty(property string) (value string, err error) {
	if d.getClosed() {
		return "", errors.ErrClosed
	}
	return
}

// GetApproximateSizes calculate approximate sizes of given ranges.
//
// Note that the returned sizes measure file system space usage, so
// if the user data compresses by a factor of ten, the returned
// sizes will be one-tenth the size of the corresponding user data size.
//
// The results may not include the sizes of recently written data.
func (d *DB) GetApproximateSizes(rr []Range) (sizes Sizes, err error) {
	if d.getClosed() {
		return nil, errors.ErrClosed
	}

	v := d.s.version()
	sizes = make(Sizes, 0, len(rr))
	for _, r := range rr {
		min := newIKey(r.Start, kMaxSeq, tSeek)
		max := newIKey(r.Limit, kMaxSeq, tSeek)
		start, err := v.approximateOffsetOf(min)
		if err != nil {
			return nil, err
		}
		limit, err := v.approximateOffsetOf(max)
		if err != nil {
			return nil, err
		}
		var size uint64
		if limit >= start {
			size = limit - start
		}
		sizes = append(sizes, size)
	}

	return
}

// CompactRange compact the underlying storage for the key range.
//
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data.  This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// Range.Start==nil is treated as a key before all keys in the database.
// Range.Limit==nil is treated as a key after all keys in the database.
// Therefore calling with Start==nil and Limit==nil will compact entire
// database.
func (d *DB) CompactRange(r Range) error {
	err := d.ok()
	if err != nil {
		return err
	}

	req := &cReq{level: -1}
	req.min = r.Start
	req.max = r.Limit

	d.creq <- req
	d.cch <- cWait

	return d.ok()
}

// Close closes the database. Snapshot and iterator are invalid
// after this call
func (d *DB) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.ErrClosed
	}
	d.closed = true
	d.mu.Unlock()

	// wake writer goroutine
	d.wch <- nil

	// wake Compaction goroutine
	d.cch <- cClose

	// wait for the WaitGroup
	d.ewg.Wait()

	d.s.tops.purgeCache()
	cache := d.s.o.GetBlockCache()
	if cache != nil {
		cache.Purge(nil)
	}

	if d.logw != nil {
		d.logw.Close()
	}
	if d.s.manifestWriter != nil {
		d.s.manifestWriter.Close()
	}

	runtime.GC()

	return d.err
}
