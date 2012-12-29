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

type cMem struct {
	s   *session
	rec *sessionRecord
}

func newCMem(s *session) *cMem {
	return &cMem{s, new(sessionRecord)}
}

func (c *cMem) flush(mem *memdb.DB, level int) error {
	s := c.s

	// Write memdb to table
	t, n, err := s.tops.createFrom(mem.NewIterator())
	if err != nil {
		return err
	}

	min, max := t.smallest.ukey(), t.largest.ukey()
	if level < 0 {
		level = s.version().pickLevel(min, max)
	}
	c.rec.addTableFile(level, t)

	s.printf("MemCompaction: table created, level=%d num=%d size=%d entries=%d min=%q max=%q",
		level, t.file.Number(), t.size, n, shorten(string(min)), shorten(string(max)))

	return nil
}

func (c *cMem) reset() {
	c.rec = new(sessionRecord)
}

func (c *cMem) commit(log, seq uint64) error {
	c.rec.setLogNum(log)
	c.rec.setSequence(seq)

	// Commit changes
	return c.s.commit(c.rec)
}

type cReq struct {
	level    int
	min, max iKey
}

type cSignal int

const (
	cWait cSignal = iota
	cSched
	cClose
)

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
	sequence  uint64
	fSequence uint64
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
		s:        s,
		cch:      make(chan cSignal),
		creq:     make(chan *cReq),
		wch:      make(chan *Batch),
		eack:     make(chan struct{}),
		sequence: s.st.sequence,
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
			d.fSequence = d.sequence

			if mem.Len() > 0 {
				err = cm.flush(mem, 0)
				if err != nil {
					return
				}
			}

			err = cm.commit(file.Number(), d.fSequence)
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
			d.sequence, err = replayBatch(lr.Record(), mb)
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

	d.fSequence = d.sequence

	err = cm.commit(d.logFile.Number(), d.fSequence)
	if err != nil {
		return
	}

	if fLogFile != nil {
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
	d.mem = memdb.New(s.cmp)

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
		return ErrClosed
	}
	return nil
}

func (d *DB) transact(f func() error) {
	s := d.s

	exit := func() {
		s.print("Transact: exiting")

		// dry out, until found close signal
		for signal := range d.cch {
			if signal == cClose {
				break
			}
		}
		panic(d)
	}

	for {
		if d.getClosed() {
			exit()
		}
		err := f()
		if err != d.err {
			d.setError(err)
		}
		if err == nil {
			return
		}
		s.printf("Transact: err=`%v'", err)
		// dry the channel
	drain:
		for {
			select {
			case <-d.cch:
			default:
				break drain
			}
		}
		if d.getClosed() {
			exit()
		}
		time.Sleep(time.Second)
	}
}

func (d *DB) memCompaction() {
	s := d.s
	c := newCMem(s)

	s.printf("MemCompaction: started, size=%d", d.fmem.Size())

	d.transact(func() (err error) {
		return c.flush(d.fmem, -1)
	})

	d.transact(func() (err error) {
		return c.commit(d.logFile.Number(), d.fSequence)
	})

	// drop frozen mem
	d.dropFrozenMem()

	c = nil
	runtime.GC()
}

func (d *DB) doCompaction(c *compaction, noTrivial bool) {
	s := d.s
	ucmp := s.cmp.cmp

	s.printf("Compaction: compacting, level=%d tables=%d, level=%d tables=%d",
		c.level, len(c.tables[0]), c.level+1, len(c.tables[1]))

	rec := new(sessionRecord)
	rec.addCompactPointer(c.level, c.max)

	if !noTrivial && c.trivial() {
		t := c.tables[0][0]
		rec.deleteTable(c.level, t.file.Number())
		rec.addTableFile(c.level+1, t)
		d.transact(func() (err error) {
			return s.commit(rec)
		})
		s.printf("Compaction: table level changed, num=%d from=%d to=%d",
			t.file.Number(), c.level, c.level+1)
		return
	}

	var snapUkey []byte
	var snapHasUkey bool
	var snapSeq uint64
	var snapIter int
	minSeq := d.minSnapshot()
	var tw *tWriter

	finish := func() error {
		t, err := tw.finish()
		if err != nil {
			return err
		}
		rec.addTableFile(c.level+1, t)
		min, max := shorten(string(t.smallest.ukey())), shorten(string(t.largest.ukey()))
		s.printf("Compaction: table created, level=%d num=%d size=%d entries=%d min=%q max=%q",
			c.level+1, t.file.Number(), t.size, tw.tw.Len(), min, max)
		return nil
	}

	d.transact(func() (err error) {
		tw = nil
		ukey := snapUkey
		hasUkey := snapHasUkey
		seq := snapSeq
		snapSched := snapIter == 0

		defer func() {
			if err != nil && tw != nil {
				tw.drop()
				tw = nil
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
			drain:
				for {
					select {
					case signal := <-d.cch:
						if signal == cClose {
							panic(d)
						}
					default:
						break drain
					}
				}
			}

			key := iKey(iter.Key())

			if c.shouldStopBefore(key) && tw != nil {
				err = finish()
				if err != nil {
					return
				}
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
			if tw.tw.Size() >= kMaxTableSize {
				err = finish()
				if err != nil {
					return
				}
				snapSched = true
				tw = nil
			}
		}

		return
	})

	// Finish last table
	if tw != nil {
		d.transact(func() (err error) {
			return finish()
		})
		tw = nil
	}

	s.print("Compaction: done")

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
			s.tops.remove(t)
		}
	}

	runtime.GC()
}

func (d *DB) compaction() {
	s := d.s

	defer func() {
		if x := recover(); x != nil {
			if x != d {
				panic(x)
			}
		}
		// dry the channel
	drain:
		for {
			select {
			case <-d.cch:
			case <-d.creq:
			default:
				break drain
			}
		}
		d.eack <- struct{}{}
		close(d.cch)
	}()

	for {
		var creq *cReq
		select {
		case signal := <-d.cch:
			switch signal {
			case cWait:
				continue
			case cSched:
			case cClose:
				return
			}
		case creq = <-d.creq:
			if creq == nil {
				continue
			}

			s.printf("CompactRange: ordered, level=%d", creq.level)

			if d.hasFrozenMem() {
				d.memCompaction()
			}

			if creq.level >= 0 {
				c := s.getCompactionRange(creq.level, creq.min, creq.max)
				if c != nil {
					d.doCompaction(c, true)
				}
			} else {
				v := s.version()
				maxLevel := 1
				for i, tt := range v.tables[1:] {
					if tt.isOverlaps(creq.min, creq.max, true, s.cmp) {
						maxLevel = i + 1
					}
				}
				for i := 0; i < maxLevel; i++ {
					c := s.getCompactionRange(i, creq.min, creq.max)
					if c != nil {
						d.doCompaction(c, true)
					}
				}
			}
			s.print("CompactRange: done")
		}

		for a, b := true, true; a || b; {
			a, b = false, false
			if d.hasFrozenMem() {
				d.memCompaction()
				a = true
				continue
			}

			if s.needCompaction() {
				d.doCompaction(s.pickCompaction(), false)
				b = true
			}
		}
	}
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

		// set batch first sequence number relative from last sequence
		// don't hold lock here, since this goroutine
		// is the only one that modify the sequence number
		seq := d.sequence
		b.sequence = seq + 1

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

type snapshot struct {
	d        *DB
	entry    *snapEntry
	released uint32
}

func (p *snapshot) Get(key []byte, ro *leveldb.ReadOptions) (value []byte, err error) {
	d := p.d
	s := d.s

	if d.getClosed() {
		return nil, ErrClosed
	}

	ucmp := s.cmp.cmp
	ikey := newIKey(key, p.entry.sequence, tSeek)
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

	if cState && !d.getClosed() {
		// schedule compaction
		select {
		case d.cch <- cSched:
		default:
		}
	}

	return
}

func (p *snapshot) NewIterator(ro *leveldb.ReadOptions) leveldb.Iterator {
	d := p.d
	s := d.s

	if d.getClosed() {
		return &leveldb.EmptyIterator{ErrClosed}
	}

	return newDBIter(p.entry.sequence, d.newRawIterator(ro), s.cmp.cmp)
}

func (p *snapshot) Release() {
	if atomic.CompareAndSwapUint32(&p.released, 0, 1) {
		p.d.releaseSnapshot(p.entry)
	}
}
