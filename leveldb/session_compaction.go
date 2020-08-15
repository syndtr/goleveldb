// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	undefinedCompaction = iota
	level0Compaction
	nonLevel0Compaction
	seekCompaction
)

func (s *session) pickMemdbLevel(umin, umax []byte, maxLevel int) int {
	v := s.version()
	defer v.release()
	return v.pickMemdbLevel(umin, umax, maxLevel)
}

func (s *session) flushMemdb(rec *sessionRecord, mdb *memdb.DB, maxLevel int) (int, error) {
	// Create sorted table.
	iter := mdb.NewIterator(nil)
	defer iter.Release()
	t, n, err := s.tops.createFrom(iter)
	if err != nil {
		return 0, err
	}

	// Pick level other than zero can cause compaction issue with large
	// bulk insert and delete on strictly incrementing key-space. The
	// problem is that the small deletion markers trapped at lower level,
	// while key/value entries keep growing at higher level. Since the
	// key-space is strictly incrementing it will not overlaps with
	// higher level, thus maximum possible level is always picked, while
	// overlapping deletion marker pushed into lower level.
	// See: https://github.com/syndtr/goleveldb/issues/127.
	flushLevel := s.pickMemdbLevel(t.imin.ukey(), t.imax.ukey(), maxLevel)
	rec.addTableFile(flushLevel, t)

	s.logf("memdb@flush created L%d@%d N·%d S·%s %q:%q", flushLevel, t.fd.Num, n, shortenb(int(t.size)), t.imin, t.imax)
	return flushLevel, nil
}

// pickFirst picks the seed file for compaction if there is no ongoing compaction
// in this level. The pick algorithm is very simple: select the first file with the
// max key greater than compPtr as the seed file. If the compPtr is nil, than pick
// the first one.
func (s *session) pickFirst(level int, v *version, ctx *compactionContext) *compaction {
	var (
		cptr   = s.getCompPtr(level)
		tables = v.levels[level]
	)
	typ := level0Compaction
	if level != 0 {
		typ = nonLevel0Compaction
	}
	// If it's level0 compaction(file overlapped) or the cptr is nil,
	// iterate from the beginning.
	if level == 0 || cptr == nil {
		for _, t := range tables {
			c := newCompaction(s, v, level, tFiles{t}, typ, ctx)
			if c != nil {
				return c
			}
		}
		return nil
	}
	// Binary search the proper start position
	start := sort.Search(len(tables), func(i int) bool {
		return s.icmp.Compare(tables[i].imax, cptr) > 0
	})
	for i := start; i < len(tables); i++ {
		c := newCompaction(s, v, level, tFiles{tables[i]}, typ, ctx)
		if c != nil {
			return c
		}
	}
	for i := 0; i < start; i++ {
		c := newCompaction(s, v, level, tFiles{tables[i]}, typ, ctx)
		if c != nil {
			return c
		}
	}
	return nil
}

// pickMore picks the seed file for compaction if there are ongoing compactions
// in this level.
func (s *session) pickMore(level int, v *version, ctx *compactionContext) *compaction {
	var (
		reverse      bool
		start, limit internalKey
	)
	typ := level0Compaction
	if level != 0 {
		typ = nonLevel0Compaction
	}
	cs := ctx.get(level)
	if len(cs) == 0 {
		return nil // Should never happen
	}

	limit = cs[len(cs)-1].imax
	start = cs[0].imax
	if s.icmp.Compare(start, limit) > 0 {
		reverse = true
		start, limit = limit, start
	}

	tables := v.levels[level]
	if !reverse {
		p := sort.Search(len(tables), func(i int) bool {
			return s.icmp.Compare(tables[i].imax, limit) > 0
		})
		for i := p; i < len(tables); i++ {
			c := newCompaction(s, v, level, tFiles{tables[i]}, typ, ctx)
			if c != nil {
				return c
			}
		}
		for _, t := range tables {
			if s.icmp.Compare(t.imax, start) >= 0 {
				break
			}
			c := newCompaction(s, v, level, tFiles{t}, typ, ctx)
			if c != nil {
				return c
			}
		}
		return nil
	} else {
		p := sort.Search(len(tables), func(i int) bool {
			return s.icmp.Compare(tables[i].imax, start) > 0
		})
		for i := p; i < len(tables); i++ {
			if s.icmp.Compare(tables[i].imax, limit) >= 0 {
				break
			}
			c := newCompaction(s, v, level, tFiles{tables[i]}, typ, ctx)
			if c != nil {
				return c
			}
		}
		return nil
	}
}

func (s *session) pickCompactionByLevel(level int, ctx *compactionContext) (comp *compaction) {
	v := s.version()
	defer func() {
		if comp == nil {
			v.release()
		}
	}()
	if len(ctx.get(level)) == 0 {
		return s.pickFirst(level, v, ctx)
	}
	return s.pickMore(level, v, ctx)
}

func (s *session) pickCompactionByTable(level int, table *tFile, ctx *compactionContext) (comp *compaction) {
	v := s.version()
	defer func() {
		if comp == nil {
			v.release()
		}
	}()
	return newCompaction(s, v, level, []*tFile{table}, seekCompaction, ctx)
}

// Create compaction from given level and range; need external synchronization.
func (s *session) getCompactionRange(sourceLevel int, umin, umax []byte, noLimit bool) *compaction {
	v := s.version()

	if sourceLevel >= len(v.levels) {
		v.release()
		return nil
	}

	t0 := v.levels[sourceLevel].getOverlaps(nil, s.icmp, umin, umax, sourceLevel == 0)
	if len(t0) == 0 {
		v.release()
		return nil
	}

	// Avoid compacting too much in one shot in case the range is large.
	// But we cannot do this for level-0 since level-0 files can overlap
	// and we must not pick one file and drop another older file if the
	// two files overlap.
	if !noLimit && sourceLevel > 0 {
		limit := int64(v.s.o.GetCompactionSourceLimit(sourceLevel))
		total := int64(0)
		for i, t := range t0 {
			total += t.size
			if total >= limit {
				s.logf("table@compaction limiting F·%d -> F·%d", len(t0), i+1)
				t0 = t0[:i+1]
				break
			}
		}
	}

	typ := level0Compaction
	if sourceLevel != 0 {
		typ = nonLevel0Compaction
	}
	return newCompaction(s, v, sourceLevel, t0, typ, nil)
}

func newCompaction(s *session, v *version, sourceLevel int, t0 tFiles, typ int, ctx *compactionContext) *compaction {
	c := &compaction{
		s:             s,
		v:             v,
		typ:           typ,
		sourceLevel:   sourceLevel,
		levels:        [2]tFiles{t0, nil},
		maxGPOverlaps: int64(s.o.GetCompactionGPOverlaps(sourceLevel)),
		tPtrs:         make([]int, len(v.levels)),
	}
	if !c.expand(ctx) {
		return nil
	}
	c.save()
	return c
}

// compaction represent a compaction state.
type compaction struct {
	s *session
	v *version

	id            int64
	typ           int
	sourceLevel   int
	levels        [2]tFiles
	maxGPOverlaps int64

	gp                tFiles
	gpi               int
	seenKey           bool
	gpOverlappedBytes int64
	imin, imax        internalKey
	tPtrs             []int
	released          bool

	snapGPI               int
	snapSeenKey           bool
	snapGPOverlappedBytes int64
	snapTPtrs             []int
}

func (c *compaction) save() {
	c.snapGPI = c.gpi
	c.snapSeenKey = c.seenKey
	c.snapGPOverlappedBytes = c.gpOverlappedBytes
	c.snapTPtrs = append(c.snapTPtrs[:0], c.tPtrs...)
}

func (c *compaction) restore() {
	c.gpi = c.snapGPI
	c.seenKey = c.snapSeenKey
	c.gpOverlappedBytes = c.snapGPOverlappedBytes
	c.tPtrs = append(c.tPtrs[:0], c.snapTPtrs...)
}

func (c *compaction) release() {
	if !c.released {
		c.released = true
		c.v.release()
	}
}

// Expand compacted tables; need external synchronization.
func (c *compaction) expand(ctx *compactionContext) bool {
	limit := int64(c.s.o.GetCompactionExpandLimit(c.sourceLevel))
	vt0 := c.v.levels[c.sourceLevel]
	vt1 := tFiles{}
	if level := c.sourceLevel + 1; level < len(c.v.levels) {
		vt1 = c.v.levels[level]
	}

	t0, t1 := c.levels[0], c.levels[1]
	imin, imax := t0.getRange(c.s.icmp, c.sourceLevel == 0)

	// For non-zero levels, the ukey can't hop across tables at all.
	if c.sourceLevel == 0 {
		// We expand t0 here just incase ukey hop across tables.
		t0 = vt0.getOverlaps(t0, c.s.icmp, imin.ukey(), imax.ukey(), true)
		if len(t0) != len(c.levels[0]) {
			imin, imax = t0.getRange(c.s.icmp, true)
		}
	}
	if c.sourceLevel != 0 && ctx != nil {
		// Ensure the source level files are not the input of other compactions.
		if ctx.removing(c.sourceLevel).hasFiles(t0) {
			return false
		}
		if ctx.recreating(c.sourceLevel).hasFiles(t0) {
			return false
		}
	}
	t1 = vt1.getOverlaps(t1, c.s.icmp, imin.ukey(), imax.ukey(), false)

	// If the overlapped tables in level n+1 are not available, abort the expansion
	if ctx != nil {
		if ctx.recreating(c.sourceLevel + 1).hasFiles(t1) {
			return false
		}
		if ctx.removing(c.sourceLevel + 1).hasFiles(t1) {
			return false
		}
	}
	// Get entire range covered by compaction.
	amin, amax := append(t0, t1...).getRange(c.s.icmp, true)

	// See if we can grow the number of inputs in "sourceLevel" without
	// changing the number of "sourceLevel+1" files we pick up.
	if len(t1) > 0 {
		exp0 := vt0.getOverlaps(nil, c.s.icmp, amin.ukey(), amax.ukey(), c.sourceLevel == 0)
		if len(exp0) > len(t0) && t1.size()+exp0.size() < limit {
			var skip bool
			if ctx != nil {
				skip = ctx.removing(c.sourceLevel).hasFiles(exp0) || ctx.recreating(c.sourceLevel).hasFiles(exp0)
			}
			if !skip {
				xmin, xmax := exp0.getRange(c.s.icmp, c.sourceLevel == 0)
				exp1 := vt1.getOverlaps(nil, c.s.icmp, xmin.ukey(), xmax.ukey(), false)
				if len(exp1) == len(t1) {
					c.s.logf("table@compaction expanding L%d+L%d (F·%d S·%s)+(F·%d S·%s) -> (F·%d S·%s)+(F·%d S·%s)",
						c.sourceLevel, c.sourceLevel+1, len(t0), shortenb(int(t0.size())), len(t1), shortenb(int(t1.size())),
						len(exp0), shortenb(int(exp0.size())), len(exp1), shortenb(int(exp1.size())))
					imin, imax = xmin, xmax
					t0, t1 = exp0, exp1
					amin, amax = append(t0, t1...).getRange(c.s.icmp, c.sourceLevel == 0)
				}
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == sourceLevel+1; grandparent == sourceLevel+2)
	//
	// Note the tables overlapped in the grandparent level may are removing.
	// We don't care about it seems the only downside is we split in tables
	// in the parent level but actually we don't need.
	if level := c.sourceLevel + 2; level < len(c.v.levels) {
		c.gp = c.v.levels[level].getOverlaps(c.gp, c.s.icmp, amin.ukey(), amax.ukey(), false)
	}

	c.levels[0], c.levels[1] = t0, t1
	c.imin, c.imax = imin, imax

	return true
}

// Check whether compaction is trivial.
func (c *compaction) trivial() bool {
	return len(c.levels[0]) == 1 && len(c.levels[1]) == 0 && c.gp.size() <= c.maxGPOverlaps
}

func (c *compaction) baseLevelForKey(ukey []byte) bool {
	for level := c.sourceLevel + 2; level < len(c.v.levels); level++ {
		tables := c.v.levels[level]
		for c.tPtrs[level] < len(tables) {
			t := tables[c.tPtrs[level]]
			if c.s.icmp.uCompare(ukey, t.imax.ukey()) <= 0 {
				// We've advanced far enough.
				if c.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					// Key falls in this file's range, so definitely not base level.
					return false
				}
				break
			}
			c.tPtrs[level]++
		}
	}
	return true
}

func (c *compaction) shouldStopBefore(ikey internalKey) bool {
	for ; c.gpi < len(c.gp); c.gpi++ {
		gp := c.gp[c.gpi]
		if c.s.icmp.Compare(ikey, gp.imax) <= 0 {
			break
		}
		if c.seenKey {
			c.gpOverlappedBytes += gp.size
		}
	}
	c.seenKey = true

	if c.gpOverlappedBytes > c.maxGPOverlaps {
		// Too much overlap for current output; start new output.
		c.gpOverlappedBytes = 0
		return true
	}
	return false
}

// Creates an iterator.
func (c *compaction) newIterator() iterator.Iterator {
	// Creates iterator slice.
	icap := len(c.levels)
	if c.sourceLevel == 0 {
		// Special case for level-0.
		icap = len(c.levels[0]) + 1
	}
	its := make([]iterator.Iterator, 0, icap)

	// Options.
	ro := &opt.ReadOptions{
		DontFillCache: true,
		Strict:        opt.StrictOverride,
	}
	strict := c.s.o.GetStrict(opt.StrictCompaction)
	if strict {
		ro.Strict |= opt.StrictReader
	}

	for i, tables := range c.levels {
		if len(tables) == 0 {
			continue
		}

		// Level-0 is not sorted and may overlaps each other.
		if c.sourceLevel+i == 0 {
			for _, t := range tables {
				its = append(its, c.s.tops.newIterator(t, nil, ro))
			}
		} else {
			it := iterator.NewIndexedIterator(tables.newIndexIterator(c.s.tops, c.s.icmp, nil, ro), strict)
			its = append(its, it)
		}
	}

	return iterator.NewMergedIterator(its, c.s.icmp, strict)
}
