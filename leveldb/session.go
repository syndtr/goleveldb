// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// session represent a persistent database session.
type session struct {
	// Need 64-bit alignment.
	stFileNum        uint64 // current unused file number
	stJournalNum     uint64 // current journal file number; need external synchronization
	stPrevJournalNum uint64 // prev journal file number; no longer used; for compatibility with older version of leveldb
	stSeq            uint64 // last mem compacted seq; need external synchronization

	stor     storage.Storage
	storLock storage.Locker
	o        *iOptions
	cmp      *iComparer
	tops     *tOps

	manifest *journalWriter

	stCPtrs   [kNumLevels]iKey // compact pointers; need external synchronization
	stVersion unsafe.Pointer   // current version
}

func openSession(stor storage.Storage, o *opt.Options) (s *session, err error) {
	if stor == nil || o == nil {
		return nil, os.ErrInvalid
	}
	storLock, err := stor.Lock()
	if err != nil {
		return
	}
	s = new(session)
	s.stor = stor
	s.storLock = storLock
	s.cmp = &iComparer{o.GetComparer()}
	s.o = newIOptions(s, *o)
	s.tops = newTableOps(s, s.o.GetMaxOpenFiles())
	s.setVersion(&version{s: s})
	return
}

// Close session.
func (s *session) close() {
	s.tops.zapCache()
	cache := s.o.GetBlockCache()
	if cache != nil {
		cache.Purge(nil)
	}
	if s.manifest != nil {
		s.manifest.close()
	}
	s.storLock.Release()
}

// Create a new database session; need external synchronization.
func (s *session) create() error {
	// create manifest
	return s.createManifest(s.allocFileNum(), nil, nil)
}

// Recover a database session; need external synchronization.
func (s *session) recover() error {
	file, err := s.stor.GetManifest()
	if err != nil {
		return err
	}

	r, err := newJournalReader(file, true, s.journalDropFunc("manifest", file.Num()))
	if err != nil {
		return err
	}
	defer r.close()

	cmp := s.cmp.cmp.Name()
	staging := s.version_NB().newStaging()
	srec := new(sessionRecord)

	for r.journal.Next() {
		rec := new(sessionRecord)
		err = rec.decode(r.journal.Record())
		if err != nil {
			continue
		}

		if rec.hasComparer && rec.comparer != cmp {
			return errors.ErrInvalid("invalid comparer, " +
				"want '" + cmp + "', " +
				"got '" + rec.comparer + "'")
		}

		// save compact pointers
		for _, rp := range rec.compactPointers {
			s.stCPtrs[rp.level] = iKey(rp.key)
		}

		// commit record to version staging
		staging.commit(rec)

		if rec.hasJournalNum {
			srec.setJournalNum(rec.journalNum)
		}
		if rec.hasPrevJournalNum {
			srec.setPrevJournalNum(rec.prevJournalNum)
		}
		if rec.hasNextNum {
			srec.setNextNum(rec.nextNum)
		}
		if rec.hasSeq {
			srec.setSeq(rec.seq)
		}
	}

	// check for error in journal reader
	if err := r.journal.Error(); err != nil {
		return err
	}

	switch false {
	case srec.hasNextNum:
		return errors.ErrCorrupt("manifest missing next file number")
	case srec.hasJournalNum:
		return errors.ErrCorrupt("manifest missing journal file number")
	case srec.hasSeq:
		return errors.ErrCorrupt("manifest missing seq number")
	}

	s.manifest = &journalWriter{file: file}
	s.setVersion(staging.finish())
	s.setFileNum(srec.nextNum)
	s.recordCommited(srec)

	return nil
}

// Commit session; need external synchronization.
func (s *session) commit(r *sessionRecord) (err error) {
	// spawn new version based on current version
	nv := s.version_NB().spawn(r)

	if s.manifest.closed() {
		// manifest journal writer not yet created, create one
		err = s.createManifest(s.allocFileNum(), r, nv)
	} else {
		err = s.flushManifest(r)
	}

	// finally, apply new version if no error rise
	if err == nil {
		s.setVersion(nv)
	}

	return err
}

// Pick a compaction based on current state; need external synchronization.
func (s *session) pickCompaction() *compaction {
	icmp := s.cmp
	ucmp := icmp.cmp

	v := s.version_NB()

	var level int
	var t0 tFiles
	if v.cScore >= 1 {
		level = v.cLevel
		cp := s.stCPtrs[level]
		tt := v.tables[level]
		for _, t := range tt {
			if cp == nil || icmp.Compare(t.max, cp) > 0 {
				t0 = append(t0, t)
				break
			}
		}
		if len(t0) == 0 {
			t0 = append(t0, tt[0])
		}
	} else {
		if p := atomic.LoadPointer(&v.cSeek); p != nil {
			ts := (*tSet)(p)
			level = ts.level
			t0 = append(t0, ts.table)
		} else {
			return nil
		}
	}

	c := &compaction{s: s, version: v, level: level}
	if level == 0 {
		min, max := t0.getRange(icmp)
		t0 = nil
		v.tables[0].getOverlaps(min.ukey(), max.ukey(), &t0, false, ucmp)
	}

	c.tables[0] = t0
	c.expand()
	return c
}

// Create compaction from given level and range; need external synchronization.
func (s *session) getCompactionRange(level int, min, max []byte) *compaction {
	v := s.version_NB()

	var t0 tFiles
	v.tables[level].getOverlaps(min, max, &t0, level != 0, s.cmp.cmp)
	if len(t0) == 0 {
		return nil
	}

	c := &compaction{s: s, version: v, level: level}
	c.tables[0] = t0
	c.expand()
	return c
}

// compaction represent a compaction state
type compaction struct {
	s       *session
	version *version

	level  int
	tables [2]tFiles

	gp              tFiles
	gpidx           int
	seenKey         bool
	overlappedBytes uint64
	min, max        iKey

	tPtrs [kNumLevels]int
}

// Expand compacted tables; need external synchronization.
func (c *compaction) expand() {
	s := c.s
	v := c.version
	icmp := s.cmp
	ucmp := icmp.cmp

	level := c.level
	vt0, vt1 := v.tables[level], v.tables[level+1]

	t0, t1 := c.tables[0], c.tables[1]
	min, max := t0.getRange(icmp)
	vt1.getOverlaps(min.ukey(), max.ukey(), &t1, true, ucmp)

	// Get entire range covered by compaction
	amin, amax := append(t0, t1...).getRange(icmp)

	// See if we can grow the number of inputs in "level" without
	// changing the number of "level+1" files we pick up.
	if len(t1) > 0 {
		var exp0 tFiles
		vt0.getOverlaps(amin.ukey(), amax.ukey(), &exp0, level != 0, ucmp)
		if len(exp0) > len(t0) && t1.size()+exp0.size() < kExpCompactionMaxBytes {
			var exp1 tFiles
			xmin, xmax := exp0.getRange(icmp)
			vt1.getOverlaps(xmin.ukey(), xmax.ukey(), &exp1, true, ucmp)
			if len(exp1) == len(t1) {
				s.printf("Compaction: expanding, level=%d from=`%d+%d (%d+%d bytes)' to=`%d+%d (%d+%d bytes)'",
					level, len(t0), len(t1), t0.size(), t1.size(),
					len(exp0), len(exp1), exp0.size(), exp1.size())
				min, max = xmin, xmax
				t0, t1 = exp0, exp1
				amin, amax = append(t0, t1...).getRange(icmp)
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == level+1; grandparent == level+2)
	if level+2 < kNumLevels {
		v.tables[level+2].getOverlaps(amin.ukey(), amax.ukey(), &c.gp, true, ucmp)
	}

	c.tables[0], c.tables[1] = t0, t1
	c.min, c.max = min, max
}

// Check whether compaction is trivial.
func (c *compaction) trivial() bool {
	return len(c.tables[0]) == 1 && len(c.tables[1]) == 0 && c.gp.size() <= kMaxGrandParentOverlapBytes
}

func (c *compaction) isBaseLevelForKey(key []byte) bool {
	s := c.s
	v := c.version
	ucmp := s.cmp.cmp
	for level, tt := range v.tables[c.level+2:] {
		for c.tPtrs[level] < len(tt) {
			t := tt[c.tPtrs[level]]
			if ucmp.Compare(key, t.max.ukey()) <= 0 {
				// We've advanced far enough
				if ucmp.Compare(key, t.min.ukey()) >= 0 {
					// Key falls in this file's range, so definitely not base level
					return false
				}
				break
			}
			c.tPtrs[level]++
		}
	}
	return true
}

func (c *compaction) shouldStopBefore(key iKey) bool {
	icmp := c.s.cmp
	for ; c.gpidx < len(c.gp); c.gpidx++ {
		gp := c.gp[c.gpidx]
		if icmp.Compare(key, gp.max) <= 0 {
			break
		}
		if c.seenKey {
			c.overlappedBytes += gp.size
		}
	}
	c.seenKey = true

	if c.overlappedBytes > kMaxGrandParentOverlapBytes {
		// Too much overlap for current output; start new output
		c.overlappedBytes = 0
		return true
	}
	return false
}

func (c *compaction) newIterator() iterator.Iterator {
	s := c.s
	icmp := s.cmp

	level := c.level
	icap := 2
	if c.level == 0 {
		icap = len(c.tables[0]) + 1
	}
	its := make([]iterator.Iterator, 0, icap)

	ro := &opt.ReadOptions{
		Flag: opt.RFDontFillCache,
	}
	if s.o.HasFlag(opt.OFParanoidCheck) {
		ro.Flag |= opt.RFVerifyChecksums
	}

	for i, tt := range c.tables {
		if len(tt) == 0 {
			continue
		}

		if level+i == 0 {
			for _, t := range tt {
				its = append(its, s.tops.newIterator(t, ro))
			}
		} else {
			it := iterator.NewIndexedIterator(tt.newIndexIterator(s.tops, icmp, ro))
			its = append(its, it)
		}
	}

	return iterator.NewMergedIterator(its, icmp)
}
