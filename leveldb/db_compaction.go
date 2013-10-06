// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
)

var (
	errTransactExiting = errors.New("leveldb: transact exiting")
)

type cStats struct {
	sync.Mutex
	duration time.Duration
	read     uint64
	write    uint64
}

func (p *cStats) add(n *cStatsStaging) {
	p.Lock()
	p.duration += n.duration
	p.read += n.read
	p.write += n.write
	p.Unlock()
}

func (p *cStats) get() (duration time.Duration, read, write uint64) {
	p.Lock()
	defer p.Unlock()
	return p.duration, p.read, p.write
}

type cStatsStaging struct {
	start    time.Time
	duration time.Duration
	on       bool
	read     uint64
	write    uint64
}

func (p *cStatsStaging) startTimer() {
	if !p.on {
		p.start = time.Now()
		p.on = true
	}
}

func (p *cStatsStaging) stopTimer() {
	if p.on {
		p.duration += time.Since(p.start)
		p.on = false
	}
}

type cReq struct {
	level    int
	min, max iKey
	cch      chan<- struct{}
}

type cMem struct {
	s     *session
	level int
	rec   *sessionRecord
}

func newCMem(s *session) *cMem {
	return &cMem{s: s, rec: new(sessionRecord)}
}

func (c *cMem) flush(mem *memdb.DB, level int) error {
	s := c.s

	// Write memdb to table
	t, n, err := s.tops.createFrom(mem.NewIterator())
	if err != nil {
		return err
	}

	if level < 0 {
		level = s.version_NB().pickLevel(t.min.ukey(), t.max.ukey())
	}
	c.rec.addTableFile(level, t)

	s.logf("mem@flush created L%d@%d N·%d S·%s %q:%q", level, t.file.Num(), n, shortenb(int(t.size)), t.min, t.max)

	c.level = level
	return nil
}

func (c *cMem) reset() {
	c.rec = new(sessionRecord)
}

func (c *cMem) commit(journal, seq uint64) error {
	c.rec.setJournalNum(journal)
	c.rec.setSeq(seq)
	// Commit changes
	return c.s.commit(c.rec)
}

func (d *DB) compactionError() {
	var err error
noerr:
	for {
		select {
		case _, _ = <-d.closeCh:
			return
		case err = <-d.compErrSetCh:
			if err != nil {
				goto haserr
			}
		}
	}
haserr:
	for {
		select {
		case _, _ = <-d.closeCh:
			return
		case err = <-d.compErrSetCh:
			if err == nil {
				goto noerr
			}
		case d.compErrCh <- err:
		}
	}
}

func (d *DB) transact(name string, exec, rollback func() error) {
	s := d.s
	defer func() {
		if x := recover(); x != nil {
			if x == errTransactExiting && rollback != nil {
				if err := rollback(); err != nil {
					s.logf("%s rollback error %q", name, err)
				}
			}
			panic(x)
		}
	}()
	for {
		if d.isClosed() {
			s.logf("%s exiting", name)
			panic(errTransactExiting)
		}
		err := exec()
		select {
		case _, _ = <-d.closeCh:
			s.logf("%s exiting", name)
			panic(errTransactExiting)
		case d.compErrSetCh <- err:
		}
		if err == nil {
			return
		}
		s.logf("%s error %q", name, err)
		time.Sleep(time.Second)
	}
}

func (d *DB) memCompaction() {
	s := d.s
	c := newCMem(s)
	stats := new(cStatsStaging)
	mem := d.getFrozenMem()

	s.logf("mem@flush N·%d S·%s", mem.Len(), shortenb(mem.Size()))

	d.transact("mem@flush", func() (err error) {
		stats.startTimer()
		defer stats.stopTimer()
		return c.flush(mem, -1)
	}, func() error {
		for _, r := range c.rec.addedTables {
			s.logf("mem@flush rollback @%d", r.num)
			f := s.getTableFile(r.num)
			if err := f.Remove(); err != nil {
				return err
			}
		}
		return nil
	})

	d.transact("mem@commit", func() (err error) {
		stats.startTimer()
		defer stats.stopTimer()
		return c.commit(d.journalFile.Num(), d.frozenSeq)
	}, nil)

	s.logf("mem@flush commited F·%d T·%v", len(c.rec.addedTables), stats.duration)

	for _, r := range c.rec.addedTables {
		stats.write += r.size
	}
	d.compStats[c.level].add(stats)

	// drop frozen mem
	d.dropFrozenMem()

	c = nil
}

func (d *DB) doCompaction(c *compaction, noTrivial bool) {
	s := d.s
	ucmp := s.cmp.cmp

	rec := new(sessionRecord)
	rec.addCompactionPointer(c.level, c.max)

	if !noTrivial && c.trivial() {
		t := c.tables[0][0]
		s.logf("table@move L%d@%d -> L%d", c.level, t.file.Num(), c.level+1)
		rec.deleteTable(c.level, t.file.Num())
		rec.addTableFile(c.level+1, t)
		d.transact("table@move", func() (err error) {
			return s.commit(rec)
		}, nil)
		return
	}

	var stats [2]cStatsStaging
	for i, tt := range c.tables {
		for _, t := range tt {
			stats[i].read += t.size
			// Insert deleted tables into record
			rec.deleteTable(c.level+i, t.file.Num())
		}
	}
	sourceSize := int(stats[0].read + stats[1].read)
	minSeq := d.minSeq()
	s.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.level, len(c.tables[0]), c.level+1, len(c.tables[1]), shortenb(sourceSize), minSeq)

	var snapUkey []byte
	var snapHasUkey bool
	var snapSeq uint64
	var snapIter int
	var snapDropCnt int
	var dropCnt int
	d.transact("table@build", func() (err error) {
		ukey := append([]byte{}, snapUkey...)
		hasUkey := snapHasUkey
		lseq := snapSeq
		dropCnt = snapDropCnt
		snapSched := snapIter == 0

		var tw *tWriter
		finish := func() error {
			t, err := tw.finish()
			if err != nil {
				return err
			}
			rec.addTableFile(c.level+1, t)
			stats[1].write += t.size
			s.logf("table@compaction created L%d@%d N·%d S·%s %q:%q", c.level+1, t.file.Num(), tw.tw.EntriesLen(), shortenb(int(t.size)), t.min, t.max)
			return nil
		}

		defer func() {
			stats[1].stopTimer()
			if tw != nil {
				tw.drop()
				tw = nil
			}
		}()

		stats[1].startTimer()
		iter := c.newIterator()
		defer iter.Release()
		for i := 0; iter.Next(); i++ {
			// Skip until last state.
			if i < snapIter {
				continue
			}

			// Prioritize memdb compaction.
			select {
			case _, _ = <-d.closeCh:
				err = ErrClosed
				return
			case cch := <-d.compMemCh:
				stats[1].stopTimer()
				d.memCompaction()
				d.compMemAckCh <- struct{}{}
				if cch != nil {
					cch <- struct{}{}
				}
				stats[1].startTimer()
			default:
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
				snapUkey = append(snapUkey[:0], ukey...)
				snapHasUkey = hasUkey
				snapSeq = lseq
				snapIter = i
				snapDropCnt = dropCnt
				snapSched = false
			}

			// defered error checking from above new table creation
			if err != nil {
				return
			}

			if seq, t, ok := key.parseNum(); !ok {
				// Don't drop error keys
				ukey = ukey[:0]
				hasUkey = false
				lseq = kMaxSeq
			} else {
				if !hasUkey || ucmp.Compare(key.ukey(), ukey) != 0 {
					// First occurrence of this user key
					ukey = append(ukey[:0], key.ukey()...)
					hasUkey = true
					lseq = kMaxSeq
				}

				drop := false
				if lseq <= minSeq {
					// Dropped because newer entry for same user key exist
					drop = true // (A)
				} else if t == tDel && seq <= minSeq && c.isBaseLevelForKey(ukey) {
					// For this user key:
					// (1) there is no data in higher levels
					// (2) data in lower levels will have larger seq numbers
					// (3) data in layers that are being compacted here and have
					//     smaller seq numbers will be dropped in the next
					//     few iterations of this loop (by rule (A) above).
					// Therefore this deletion marker is obsolete and can be dropped.
					drop = true
				}

				lseq = seq
				if drop {
					dropCnt++
					continue
				}
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
			if tw.tw.BytesLen() >= kMaxTableSize {
				err = finish()
				if err != nil {
					return
				}
				snapSched = true
				tw = nil
			}
		}

		// Finish last table
		if tw != nil {
			err = finish()
			if err != nil {
				return
			}
			tw = nil
		}
		return
	}, func() error {
		for _, r := range rec.addedTables {
			s.logf("table@compaction rollback @%d", r.num)
			f := s.getTableFile(r.num)
			if err := f.Remove(); err != nil {
				return err
			}
		}
		return nil
	})

	// Commit changes
	d.transact("table@commit", func() (err error) {
		stats[1].startTimer()
		defer stats[1].stopTimer()
		return s.commit(rec)
	}, nil)

	resultSize := int(int(stats[1].write))
	s.logf("table@compaction commited F%s S%s D·%d T·%v", sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), dropCnt, stats[1].duration)

	// Save compaction stats
	for i := range stats {
		d.compStats[c.level+1].add(&stats[i])
	}
}

func (d *DB) compaction() {
	s := d.s
	defer func() {
		if x := recover(); x != nil {
			if x != errTransactExiting {
				panic(x)
			}
		}
		d.closeWg.Done()
	}()
	for {
		var cch chan<- struct{}
		select {
		case _, _ = <-d.closeCh:
			return
		case cch = <-d.compMemCh:
			d.memCompaction()
			d.compMemAckCh <- struct{}{}
		case cch = <-d.compCh:
		case creq := <-d.compReqCh:
			if creq == nil {
				continue
			}
			s.logf("range compaction L%d %v:%v", creq.level, creq.min, creq.max)
			if creq.level >= 0 {
				c := s.getCompactionRange(creq.level, creq.min, creq.max)
				if c != nil {
					d.doCompaction(c, true)
				}
			} else {
				v := s.version_NB()
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
			cch = creq.cch
		}
		if s.version_NB().needCompaction() {
			d.doCompaction(s.pickCompaction(), false)
			select {
			case d.compCh <- nil:
			default:
			}
		}
		if cch != nil {
			func() {
				defer func() {
					recover()
				}()
				cch <- struct{}{}
			}()
		}
	}
}

func (d *DB) wakeCompaction(wait int) error {
	switch wait {
	case 0:
		select {
		case d.compCh <- nil:
		default:
		}
	case 1:
		select {
		case _, _ = <-d.closeCh:
			return ErrClosed
		case err := <-d.compErrCh:
			return err
		case d.compCh <- nil:
		}
	case 2:
		cch := make(chan struct{})
		defer close(cch)
		select {
		case _, _ = <-d.closeCh:
			return ErrClosed
		case err := <-d.compErrCh:
			return err
		case d.compCh <- (chan<- struct{})(cch):
		}
		select {
		case _, _ = <-d.closeCh:
			return ErrClosed
		case err := <-d.compErrCh:
			return err
		case <-cch:
		}
	}
	return nil
}
