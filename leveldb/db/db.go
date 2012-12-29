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
	"leveldb/descriptor"
	"leveldb/log"
	"leveldb/memdb"
	"os"
	"runtime"
	"sync"
	"time"
)

var ErrClosed = leveldb.ErrInvalid("database closed")

type memBatch struct {
	mem **memdb.DB
}

func (p *memBatch) put(key, value []byte, seq uint64) {
	ikey := newIKey(key, seq, tVal)
	(*(p.mem)).Put(ikey, value)
}

func (p *memBatch) delete(key []byte, seq uint64) {
	ikey := newIKey(key, seq, tDel)
	(*(p.mem)).Put(ikey, nil)
}

type DB struct {
	s    *session
	cch  chan cSignal
	creq chan *cReq
	wch  chan *Batch
	eack chan struct{}

	mu        sync.RWMutex
	mem, fmem *memdb.DB
	log       *log.Writer
	logWriter descriptor.Writer
	logFile   descriptor.File
	fLogFile  descriptor.File
	seq       uint64
	fSeq      uint64
	snapshots list.List
	err       error
	closed    bool
}

func Open(desc descriptor.Descriptor, opt *leveldb.Options) (d *DB, err error) {
	s := newSession(desc, opt)

	err = s.recover()
	if os.IsNotExist(err) && opt.HasFlag(leveldb.OFCreateIfMissing) {
		err = s.create()
	} else if err == nil && opt.HasFlag(leveldb.OFErrorIfExist) {
		println(opt.HasFlag(leveldb.OFErrorIfExist))
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
		eack: make(chan struct{}),
		seq:  s.st.seq,
	}
	d.snapshots.Init()

	err = d.recoverLog()
	if err != nil {
		return
	}

	go d.compaction()
	go d.write()
	return
}

func (d *DB) recoverLog() (err error) {
	s := d.s
	icmp := s.cmp

	var mem *memdb.DB
	mb := &memBatch{&mem}
	cm := newCMem(s)

	s.printf("LogRecovery: started, min=%d", s.st.logNum)

	var fLogFile descriptor.File
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

		if mem != nil {
			d.fSeq = d.seq

			if mem.Len() > 0 {
				err = cm.flush(mem, 0)
				if err != nil {
					return
				}
			}

			err = cm.commit(file.Number(), d.fSeq)
			if err != nil {
				return
			}

			cm.reset()

			fLogFile.Remove()
			fLogFile = nil
		}

		mem = memdb.New(icmp)

		lr := log.NewReader(r, true)
		for lr.Next() {
			d.seq, err = replayBatch(lr.Record(), mb)
			if err != nil {
				return
			}

			if mem.Size() > s.opt.GetWriteBuffer() {
				// flush to table
				err = cm.flush(mem, 0)
				if err != nil {
					return
				}

				// create new memdb
				mem = memdb.New(icmp)
			}
		}

		err = lr.Error()
		if err != nil {
			return
		}

		r.Close()
		fLogFile = file
	}

	// create new log
	err = d.newMem()
	if err != nil {
		return
	}

	if mem != nil && mem.Len() > 0 {
		err = cm.flush(mem, 0)
		if err != nil {
			return
		}
	}

	d.fSeq = d.seq

	err = cm.commit(d.logFile.Number(), d.fSeq)
	if err != nil {
		return
	}

	if fLogFile != nil {
		fLogFile.Remove()
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
		case d.mem.Size() <= s.opt.GetWriteBuffer():
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
	mb := &memBatch{&d.mem}

	for {
		b := <-d.wch
		if b == nil && d.getClosed() {
			d.eack <- struct{}{}
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
		// don't hold lock here, since this goroutine
		// is the only one that modify the seq number
		seq := d.seq
		b.seq = seq + 1

		// write log
		// don't hold lock here, since this goroutine
		// is the only one that modify and write to log
		err = d.log.Append(b.encode())
		if err != nil {
			b.done(err)
			continue
		}

		if b.sync {
			err = d.logWriter.Sync()
			if err != nil {
				b.done(err)
				continue
			}
		}

		d.mu.Lock()
		// replay batch to memdb
		b.replay(mb)
		// set last seq number
		d.seq = seq + uint64(b.len())
		d.mu.Unlock()

		// done
		b.done(nil)
	}
}

func (d *DB) Put(key, value []byte, wo *leveldb.WriteOptions) error {
	b := new(Batch)
	b.Put(key, value)
	return d.Write(b, wo)
}

func (d *DB) Delete(key []byte, wo *leveldb.WriteOptions) error {
	b := new(Batch)
	b.Delete(key)
	return d.Write(b, wo)
}

func (d *DB) Write(w leveldb.Batch, wo *leveldb.WriteOptions) (err error) {
	err = d.ok()
	if err != nil {
		return
	}

	b, ok := w.(*Batch)
	if !ok {
		return leveldb.ErrInvalid("not a *Batch")
	}

	rch := b.init(wo.HasFlag(leveldb.WFSync))
	d.wch <- b
	err = <-rch
	close(rch)
	return
}

func (d *DB) Get(key []byte, ro *leveldb.ReadOptions) (value []byte, err error) {
	p, err := d.GetSnapshot()
	if err != nil {
		return
	}
	defer p.Release()
	return p.Get(key, ro)
}

func (d *DB) newRawIterator(ro *leveldb.ReadOptions) leveldb.Iterator {
	s := d.s

	d.mu.RLock()
	ti := s.version().getIterators(ro)
	ii := make([]leveldb.Iterator, 0, len(ti)+2)
	ii = append(ii, d.mem.NewIterator())
	if d.fmem != nil {
		ii = append(ii, d.fmem.NewIterator())
	}
	ii = append(ii, ti...)
	d.mu.RUnlock()

	return leveldb.NewMergedIterator(ii, s.cmp)
}

func (d *DB) NewIterator(ro *leveldb.ReadOptions) leveldb.Iterator {
	p, err := d.GetSnapshot()
	if err != nil {
		return &leveldb.EmptyIterator{err}
	}
	return p.NewIterator(ro)
}

func (d *DB) GetSnapshot() (s leveldb.Snapshot, err error) {
	if d.getClosed() {
		return nil, ErrClosed
	}
	return &snapshot{d: d, entry: d.getSnapshot()}, nil
}

func (d *DB) GetProperty(property string) (value string, err error) {
	if d.getClosed() {
		return "", ErrClosed
	}
	return
}

func (d *DB) GetApproximateSizes(rr []leveldb.Range) (sizes leveldb.Sizes, err error) {
	if d.getClosed() {
		return nil, ErrClosed
	}

	v := d.s.version()
	sizes = make(leveldb.Sizes, 0, len(rr))
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

func (d *DB) CompactRange(r *leveldb.Range) error {
	err := d.ok()
	if err != nil {
		return err
	}

	req := &cReq{level: -1}
	if r != nil {
		req.min = r.Start
		req.max = r.Limit
	}

	d.creq <- req
	d.cch <- cWait

	return d.ok()
}

func (d *DB) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	d.closed = true
	d.mu.Unlock()

	// wake writer goroutine
	d.wch <- nil

	// wake Compaction goroutine
	d.cch <- cClose

	// wait for ack
	for i := 0; i < 2; i++ {
		<-d.eack
	}

	d.s.tops.purgeCache()
	d.s.opt.GetBlockCache().Purge(nil)

	if d.logWriter != nil {
		d.logWriter.Close()
	}
	if d.s.manifestWriter != nil {
		d.s.manifestWriter.Close()
	}

	runtime.GC()

	return d.err
}
