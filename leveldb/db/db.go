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
	"sync/atomic"
	"time"
)

type memBatch struct {
	db *DB
}

func (p *memBatch) put(key, value []byte, seq uint64) {
	ikey := newIKey(key, seq, tVal)
	p.db.mem.Put(ikey, value)
}

func (p *memBatch) delete(key []byte, seq uint64) {
	ikey := newIKey(key, seq, tDel)
	p.db.mem.Put(ikey, nil)
}

type DB struct {
	s   *session
	cch chan bool
	wch chan *Batch

	mu        sync.RWMutex
	mem, fmem *memdb.DB
	log       *log.Writer
	logWriter descriptor.Writer
	logFile   descriptor.File
	fLogFile  descriptor.File
	sequence  uint64
	fSequence uint64
	snapshots list.List
	err       error
	closed    bool
}

func Open(desc descriptor.Descriptor, opt *leveldb.Options) (d *DB, err error) {
	s := newSession(desc, opt)
	newSession := false

	err = s.recover()
	if err == os.ErrNotExist && opt.HasFlag(leveldb.OFCreateIfMissing) {
		err = s.create()
		newSession = true
	} else if err == nil && opt.HasFlag(leveldb.OFErrorIfExist) {
		err = os.ErrExist
	}
	if err != nil {
		return
	}

	d = &DB{
		s:        s,
		cch:      make(chan bool),
		wch:      make(chan *Batch),
		sequence: s.st.sequence,
	}
	d.snapshots.Init()

	if !newSession {
		err = d.recoverLog()
		if err != nil {
			return
		}
	}

	if d.mem == nil {
		err = d.newMem()
		if err != nil {
			return
		}
	}

	go d.compaction()
	go d.write()
	return
}

func (d *DB) recoverLog() (err error) {
	s := d.s
	mb := &memBatch{d}

	var mem *memdb.DB
	var logNum uint64
	memCompaction := func() error {
		// Write memdb to table
		t, err := s.tops.createFrom(mem.NewIterator())
		if err != nil {
			return err
		}

		// Drop memdb
		mem = nil

		// Build manifest record
		rec := new(sessionRecord)
		rec.setLogNum(logNum)
		rec.setSequence(d.fSequence)
		min, max := t.smallest.ukey(), t.largest.ukey()
		rec.addTableFile(s.version().pickLevel(min, max), t)

		// Commit changes
		return s.commit(rec)
	}

	var fLogFile descriptor.File
	for _, file := range s.getFiles(descriptor.TypeLog) {
		if file.Number() < s.st.logNum {
			continue
		}

		s.printf("Recovering log %d", file.Number())

		var r descriptor.Reader
		r, err = file.Open()
		if err != nil {
			return
		}

		logNum = file.Number()
		s.markFileNum(logNum)

		if mem != nil {
			d.fSequence = d.sequence

			// write prev log
			err = memCompaction()
			if err != nil {
				return
			}
			fLogFile.Remove()
			fLogFile = nil
		}

		mem = memdb.New(s.icmp)

		lr := log.NewReader(r, true)
		for lr.Next() {
			d.sequence, err = replayBatch(lr.Record(), mb)
			if err != nil {
				return
			}
		}

		err = lr.Error()
		if err != nil {
			return
		}

		r.Close()
		fLogFile = file
	}

	if mem != nil {
		// create new log
		err = d.newMem()
		if err != nil {
			return
		}

		logNum = d.logFile.Number()

		// write last log
		err = memCompaction()
		if err != nil {
			return
		}
		fLogFile.Remove()
	}

	return
}

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
	if d.logWriter != nil {
		d.logWriter.Close()
	}
	d.logWriter = w

	d.fLogFile = d.logFile
	d.logFile = file

	// new mem
	d.fmem = d.mem
	d.mem = memdb.New(s.icmp)

	d.fSequence = d.sequence

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

	d.fLogFile.Remove()
	d.fLogFile = nil
}

type snapEntry struct {
	elem     *list.Element
	sequence uint64
	ref      int
}

func (d *DB) getSnapshot() (p *snapEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	back := d.snapshots.Back()
	if back != nil {
		p = back.Value.(*snapEntry)
	}
	num := d.sequence
	if p == nil || p.sequence != num {
		p = &snapEntry{sequence: num}
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
		return d.s.sequence()
	}
	return back.Value.(*snapEntry).sequence
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
		return leveldb.ErrInvalid("database closed")
	}
	return nil
}

func (d *DB) transact(f func() error) {
	s := d.s

	for {
		if d.getClosed() {
			runtime.Goexit()
		}
		err := f()
		if err != d.err {
			d.setError(err)
		}
		if err == nil {
			return
		}
		s.print("Error during transact: ", err)
		// dry the channel
		for {
			select {
			case <-d.cch:
			default:
			}
		}
		if d.getClosed() {
			runtime.Goexit()
		}
		time.Sleep(time.Second)
	}
}

func (d *DB) memCompaction() {
	s := d.s

	s.printf("MemCompaction: started at size %d", d.fmem.Size())

	// Write memdb to table
	var t *tFile
	d.transact(func() (err error) {
		t, err = s.tops.createFrom(d.fmem.NewIterator())
		return
	})

	s.printf("MemCompaction: created table %d, size %d bytes",
		t.file.Number(), t.size)

	// Build manifest record
	rec := new(sessionRecord)
	rec.setLogNum(d.logFile.Number())
	rec.setSequence(d.fSequence)
	min, max := t.smallest.ukey(), t.largest.ukey()
	rec.addTableFile(s.version().pickLevel(min, max), t)

	// Commit changes
	d.transact(func() (err error) {
		return s.commit(rec)
	})

	// drop frozen mem
	d.dropFrozenMem()
}

func (d *DB) doCompaction() {
	s := d.s
	ucmp := s.cmp

	c := s.pickCompaction()

	s.printf("Compaction: compacting %d@%d + %d@%d tables",
		len(c.tables[0]), c.level, len(c.tables[1]), c.level+1)

	rec := new(sessionRecord)
	rec.addCompactPointer(c.level, c.max)

	if c.trivial() {
		t := c.tables[0][0]
		rec.deleteTable(c.level, t.file.Number())
		rec.addTableFile(c.level+1, t)
		d.transact(func() (err error) {
			return s.commit(rec)
		})
		s.printf("Compaction: table %d level changed from %d to %d",
			t.file.Number(), c.level, c.level+1)
		return
	}

	var snapUkey []byte
	var snapHasUkey bool
	var snapSeq uint64
	var snapIter int
	minSeq := d.minSnapshot()
	var tw *tWriter
	d.transact(func() (err error) {
		tw = nil
		ukey := snapUkey
		hasUkey := snapHasUkey
		seq := snapSeq
		snapSched := snapIter == 0

		defer func() {
			if err != nil && tw != nil {
				tw.drop()
			}
		}()

		iter := c.newIterator()
		for i := 0; iter.Next(); i++ {
			// Skip until last state
			if i < snapIter {
				continue
			}

			// Prioritize memdb compaction
			if d.hasFrozenMem() {
				d.memCompaction()
				// dry the channel
				for {
					select {
					case <-d.cch:
					default:
					}
				}
			}

			key := iKey(iter.Key())

			if c.shouldStopBefore(key) && tw != nil {
				var t *tFile
				t, err = tw.finish()
				if err != nil {
					return
				}
				rec.addTableFile(c.level+1, t)
				snapSched = true

				// create new table but don't check for error now
				tw, err = s.tops.create()
			}

			// Scheduled for snapshot, snapshot will used to retry compaction
			// if error occured.
			if snapSched {
				snapUkey = ukey
				snapHasUkey = hasUkey
				snapSeq = seq
				snapIter = i
				snapSched = false
			}

			// defered error checking from above new table creation
			if err != nil {
				return
			}

			drop := false
			ik := key.parse()
			if ik == nil {
				// Don't drop error keys
				ukey = nil
				hasUkey = false
				seq = kMaxSeq
			} else {
				if !hasUkey || ucmp.Compare(ik.ukey, ukey) != 0 {
					// First occurrence of this user key
					ukey = ik.ukey
					hasUkey = true
					seq = kMaxSeq
				}

				if seq <= minSeq {
					// Dropped because newer entry for same user key exist
					drop = true // (A)
				} else if ik.vtype == tDel && ik.sequence <= minSeq && c.isBaseLevelForKey(ik.ukey) {
					// For this user key:
					// (1) there is no data in higher levels
					// (2) data in lower levels will have larger sequence numbers
					// (3) data in layers that are being compacted here and have
					//     smaller sequence numbers will be dropped in the next
					//     few iterations of this loop (by rule (A) above).
					// Therefore this deletion marker is obsolete and can be dropped.
					drop = true
				}

				seq = ik.sequence
			}

			if drop {
				continue
			}

			// Create new table if not already
			if tw == nil {
				tw, err = s.tops.create()
				if err != nil {
					return
				}
			}

			// Write key/value into table
			err = tw.add(key, iter.Value())
			if err != nil {
				return
			}

			// Finish table if it is big enough
			if tw.tw.Size() > kMaxTableSize {
				var t *tFile
				t, err = tw.finish()
				if err != nil {
					return
				}
				rec.addTableFile(c.level+1, t)
				snapSched = true
				s.printf("Compaction: created table %d, size %d bytes, %d entries",
					t.file.Number(), t.size, tw.tw.Len())
				tw = nil
			}
		}

		return
	})

	// Finish last table
	if tw != nil {
		d.transact(func() (err error) {
			t, err := tw.finish()
			if err != nil {
				return
			}
			rec.addTableFile(c.level+1, t)
			s.printf("Compaction: created table %d, size %d bytes, %d entries",
				t.file.Number(), t.size, tw.tw.Len())
			tw = nil
			return
		})
	}

	s.printf("Compaction: compacted %d@%d + %d@%d tables",
		len(c.tables[0]), c.level, len(c.tables[1]), c.level+1)

	// Insert deleted tables into record
	for n, tt := range c.tables {
		for _, t := range tt {
			rec.deleteTable(c.level+n, t.file.Number())
		}
	}

	// Commit changes
	d.transact(func() (err error) {
		return s.commit(rec)
	})

	// Delete unused tables
	for _, tt := range c.tables {
		for _, t := range tt {
			t.file.Remove()
		}
	}
}

func (d *DB) compaction() {
	s := d.s

	for sched := range d.cch {
		if !sched {
			continue
		}

		if d.hasFrozenMem() {
			d.memCompaction()
			continue
		}

		if s.needCompaction() {
			d.doCompaction()
		}
	}
}

func (d *DB) flush() (err error) {
	s := d.s

	for {
		v := s.version()
		switch {
		case v.tLen(0) >= kL0_SlowdownWritesTrigger:
			time.Sleep(1000 * time.Microsecond)
			continue
		case d.mem.Size() <= s.opt.GetWriteBuffer():
			// still room
			return
		case d.hasFrozenMem(), v.tLen(0) >= kL0_StopWritesTrigger:
			d.cch <- false
			continue
		}

		// create new memdb and log
		err = d.newMem()
		if err != nil {
			return
		}

		// schedule compaction
		select {
		case d.cch <- true:
		default:
		}
	}

	return
}

func (d *DB) write() {
	mb := &memBatch{d}

	for {
		b := <-d.wch
		if b == nil {
			close(d.wch)
			d.flush()
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

		// set batch first sequence number relative from last sequence
		// don't hold lock here, since this gorouting
		// is the only one that modify the sequence number
		seq := d.sequence
		b.sequence = seq + 1

		// write log
		// don't hold lock here, since this gorouting
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
		// set last sequence number
		d.sequence = seq + uint64(b.len())
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
	var p leveldb.Snapshot
	p, err = d.GetSnapshot()
	if err != nil {
		return
	}
	defer p.Release()
	return p.Get(key, ro)
}

func (d *DB) NewIterator(ro *leveldb.ReadOptions) (iter leveldb.Iterator, err error) {
	var p leveldb.Snapshot
	p, err = d.GetSnapshot()
	if err != nil {
		return
	}
	defer p.Release()
	return p.NewIterator(ro)
}

func (d *DB) GetSnapshot() (s leveldb.Snapshot, err error) {
	return &snapshot{d: d, entry: d.getSnapshot()}, nil
}

func (d *DB) GetProperty(property string) (value string, err error) {
	return
}

func (d *DB) GetApproximateSizes(r *leveldb.Range, n int) (size uint64, err error) {
	return
}

func (d *DB) CompactRange(begin, end []byte) error {
	panic("not implemented")
	return nil
}

type snapshot struct {
	d        *DB
	entry    *snapEntry
	released uint32
}

func (p *snapshot) Get(key []byte, ro *leveldb.ReadOptions) (value []byte, err error) {
	d := p.d
	s := d.s

	ikey := newIKey(key, p.entry.sequence, tSeek)
	memGet := func(m *memdb.DB) bool {
		var k []byte
		k, value, err = m.Get(ikey)
		if err != nil {
			return false
		}
		ik := iKey(k)
		if s.cmp.Compare(ik.ukey(), key) != 0 {
			return false
		}
		valid, _, vt := ik.sequenceAndType()
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

	if cState {
		// schedule compaction
		select {
		case d.cch <- true:
		default:
		}
	}

	return
}

func (p *snapshot) NewIterator(ro *leveldb.ReadOptions) (iter leveldb.Iterator, err error) {
	return
}

func (p *snapshot) Release() {
	if atomic.CompareAndSwapUint32(&p.released, 0, 1) {
		p.d.releaseSnapshot(p.entry)
	}
}
