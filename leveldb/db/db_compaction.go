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
	"leveldb/memdb"
	"runtime"
	"time"
)

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

	if level < 0 {
		level = s.version().pickLevel(t.min.ukey(), t.max.ukey())
	}
	c.rec.addTableFile(level, t)

	s.printf("MemCompaction: table created, level=%d num=%d size=%d entries=%d min=%q max=%q",
		level, t.file.Number(), t.size, n, t.min, t.max)

	return nil
}

func (c *cMem) reset() {
	c.rec = new(sessionRecord)
}

func (c *cMem) commit(log, seq uint64) error {
	c.rec.setLogNum(log)
	c.rec.setSeq(seq)

	// Commit changes
	return c.s.commit(c.rec)
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
		return c.commit(d.logf.Number(), d.fseq)
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
		s.printf("Compaction: table created, level=%d num=%d size=%d entries=%d min=%q max=%q",
			c.level+1, t.file.Number(), t.size, tw.tw.Len(), t.min, t.max)
		return nil
	}

	d.transact(func() (err error) {
		tw = nil
		ukey := snapUkey
		hasUkey := snapHasUkey
		lseq := snapSeq
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
				ukey = nil
				hasUkey = false
				lseq = kMaxSeq
			} else {
				if !hasUkey || ucmp.Compare(key.ukey(), ukey) != 0 {
					// First occurrence of this user key
					ukey = key.ukey()
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
	// register to the WaitGroup
	d.ewg.Add(1)
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
		close(d.cch)
		d.ewg.Done()
	}()

	for s := d.s; true; {
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
