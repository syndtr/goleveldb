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
	timerOn  bool
	read     uint64
	write    uint64
}

func (p *cStatsStaging) startTimer() {
	if !p.timerOn {
		p.start = time.Now()
		p.timerOn = true
	}
}

func (p *cStatsStaging) stopTimer() {
	if p.timerOn {
		p.duration += time.Now().Sub(p.start)
		p.timerOn = false
	}
}

type cReq struct {
	level    int
	min, max iKey
	cch      chan<- struct{}
}

type cSignal int

const (
	cWait cSignal = iota
	cSched
	cClose
)

type cMem struct {
	s     *session
	level int
	t     *tFile
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

	s.printf("Compaction: table created, source=mem level=%d num=%d size=%d entries=%d min=%q max=%q",
		level, t.file.Num(), t.size, n, t.min, t.max)

	c.level = level
	c.t = t
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

func (d *DB) transact(name string, f func() error) {
	s := d.s
	for {
		if d.isClosed() {
			s.printf("Transact: %s: exiting", name)
			panic(errTransactExiting)
		}
		err := f()
		select {
		case _, _ = <-d.closeCh:
			s.printf("Transact: %s: exiting", name)
			panic(errTransactExiting)
		case d.compErrSetCh <- err:
		}
		if err == nil {
			return
		}
		s.printf("Transact: %s: err=%q", name, err)
		time.Sleep(time.Second)
	}
}

func (d *DB) memCompaction() {
	s := d.s
	c := newCMem(s)
	stats := new(cStatsStaging)
	mem := d.getFrozenMem()

	s.printf("MemCompaction: started, size=%d entries=%d", mem.Size(), mem.Len())

	d.transact("mem[flush]", func() (err error) {
		stats.startTimer()
		defer stats.stopTimer()
		return c.flush(mem, -1)
	})

	d.transact("mem[commit]", func() (err error) {
		stats.startTimer()
		defer stats.stopTimer()
		return c.commit(d.journalFile.Num(), d.frozenSeq)
	})

	stats.write = c.t.size
	d.compStats[c.level].add(stats)

	// drop frozen mem
	d.dropFrozenMem()

	c = nil
}

func (d *DB) doCompaction(c *compaction, noTrivial bool) {
	s := d.s
	ucmp := s.cmp.cmp

	s.printf("Compaction: compacting, level=%d tables=%d, level=%d tables=%d",
		c.level, len(c.tables[0]), c.level+1, len(c.tables[1]))

	rec := new(sessionRecord)
	rec.addCompactionPointer(c.level, c.max)

	if !noTrivial && c.trivial() {
		t := c.tables[0][0]
		rec.deleteTable(c.level, t.file.Num())
		rec.addTableFile(c.level+1, t)
		d.transact("table[rename]", func() (err error) {
			return s.commit(rec)
		})
		s.printf("Compaction: table level changed, num=%d from=%d to=%d",
			t.file.Num(), c.level, c.level+1)
		return
	}

	var snapUkey []byte
	var snapHasUkey bool
	var snapSeq uint64
	var snapIter int
	var tw *tWriter
	minSeq := d.minSeq()
	stats := new(cStatsStaging)

	finish := func() error {
		t, err := tw.finish()
		if err != nil {
			return err
		}
		rec.addTableFile(c.level+1, t)
		stats.write += t.size
		s.printf("Compaction: table created, source=table level=%d num=%d size=%d entries=%d min=%q max=%q",
			c.level+1, t.file.Num(), t.size, tw.tw.EntriesLen(), t.min, t.max)
		return nil
	}

	d.transact("table[build]", func() (err error) {
		tw = nil
		ukey := append([]byte{}, snapUkey...)
		hasUkey := snapHasUkey
		lseq := snapSeq
		snapSched := snapIter == 0

		defer func() {
			stats.stopTimer()
			if err != nil && tw != nil {
				tw.drop()
				tw = nil
			}
		}()

		stats.startTimer()
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
				stats.stopTimer()
				d.memCompaction()
				d.compMemAckCh <- struct{}{}
				if cch != nil {
					cch <- struct{}{}
				}
				stats.startTimer()
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
			return finish()
		}

		return
	})

	s.print("Compaction: build done")

	for n, tt := range c.tables {
		for _, t := range tt {
			stats.read += t.size
			// Insert deleted tables into record
			rec.deleteTable(c.level+n, t.file.Num())
		}
	}

	// Commit changes
	d.transact("table[commit]", func() (err error) {
		stats.startTimer()
		defer stats.stopTimer()
		return s.commit(rec)
	})

	s.print("Compaction: commited")

	// Save compaction stats
	d.compStats[c.level+1].add(stats)
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
			s.printf("CompactRange: ordered, level=%d", creq.level)
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
			s.print("CompactRange: done")
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
			cch <- struct{}{}
		}
	}
}
