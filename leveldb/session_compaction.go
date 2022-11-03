// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"
	"sync/atomic"

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

// 创建 SST 并添加到 rec
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

	s.logf("memdb@flush created L%d@%d N·%d S·%s %q:%q", flushLevel, t.fd.Num, n, shortenb(t.size), t.imin, t.imax)
	return flushLevel, nil
}

// Pick a compaction based on current state; need external synchronization.
func (s *session) pickCompaction() *compaction {
	v := s.version()

	var sourceLevel int
	var t0 tFiles
	var typ int
	if v.cScore >= 1 {
		sourceLevel = v.cLevel
		cptr := s.getCompPtr(sourceLevel)
		tables := v.levels[sourceLevel]
		if cptr != nil && sourceLevel > 0 {
			n := len(tables)
			if i := sort.Search(n, func(i int) bool {
				// 选取第一个 imax 大于 cptr 的 SSTable
				return s.icmp.Compare(tables[i].imax, cptr) > 0
			}); i < n {
				t0 = append(t0, tables[i])
			}
		}
		if len(t0) == 0 {
			// 如果 ctpr==nil 或 cptr已经到了最后一个 SSTable，则从头开始循环
			t0 = append(t0, tables[0])
		}
		if sourceLevel == 0 {
			typ = level0Compaction
		} else {
			typ = nonLevel0Compaction
		}
	} else {
		// 执行 seek compaction
		if p := atomic.LoadPointer(&v.cSeek); p != nil {
			ts := (*tSet)(p)
			sourceLevel = ts.level
			t0 = append(t0, ts.table)
			typ = seekCompaction
		} else {
			v.release()
			return nil
		}
	}

	return newCompaction(s, v, sourceLevel, t0, typ)
}

// Create compaction from given level and range; need external synchronization.
func (s *session) getCompactionRange(sourceLevel int, umin, umax []byte, noLimit bool) *compaction {
	v := s.version()

	if sourceLevel >= len(v.levels) {
		// sourceLevel >= 7，没有这样的 level
		v.release()
		return nil
	}

	// 注意：sourceLevel==0 时，因为 L0 上的 SSTable 可能有交叠，所以需要拓展 umin, umax 的范围
	// 比如在 L0 上，umax 涉及到了一个新的 SSTable，那么这个 SSTable 的 max 会拓宽一点，就可能会交叠上下一个新的 SSTable
	// 所以需要拓展 umin, umax 的范围，直到不可以继续拓展
	// 使用的是 ukey，完整的 ukey 要完整地被 dump，不能出现部分 ukey 被 dump 了，这样会出现查询的错误
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
	return newCompaction(s, v, sourceLevel, t0, typ)
}

func newCompaction(s *session, v *version, sourceLevel int, t0 tFiles, typ int) *compaction {
	c := &compaction{
		s:             s,
		v:             v,
		typ:           typ,
		sourceLevel:   sourceLevel,
		levels:        [2]tFiles{t0, nil},
		maxGPOverlaps: int64(s.o.GetCompactionGPOverlaps(sourceLevel)),
		tPtrs:         make([]int, len(v.levels)),
	}
	c.expand()
	c.save()
	return c
}

// compaction represent a compaction state.
type compaction struct {
	s *session
	v *version

	typ           int       // compaction 的 type: L0, non-L0, seek compaction
	sourceLevel   int       // compaction 的 source level，发起 compaction 的 level
	levels        [2]tFiles // 参与 compaction 的两个 level 上的文件
	maxGPOverlaps int64

	gp                tFiles // sourceLevel+2 上的、与某次 compaction expand 之后的 range 相重叠的文件，gp 的意思是 grad parent
	gpi               int
	seenKey           bool
	gpOverlappedBytes int64
	imin, imax        internalKey // 参与 compaction 的 sourceLevel 上的 internalKey range
	tPtrs             []int       // tPtrs[level] 是 level 上的一个 SSTable 的 index，用于在 compaction 的时候快速判断 ukey 是否不会在 sourceLevel+2 及更深的 level 上出现
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
func (c *compaction) expand() {
	limit := int64(c.s.o.GetCompactionExpandLimit(c.sourceLevel))
	vt0 := c.v.levels[c.sourceLevel]
	vt1 := tFiles{}
	if level := c.sourceLevel + 1; level < len(c.v.levels) {
		vt1 = c.v.levels[level]
	}

	t0, t1 := c.levels[0], c.levels[1]
	imin, imax := t0.getRange(c.s.icmp)

	// For non-zero levels, the ukey can't hop across tables at all.
	if c.sourceLevel == 0 {
		// We expand t0 here just incase ukey hop across tables.
		t0 = vt0.getOverlaps(t0, c.s.icmp, imin.ukey(), imax.ukey(), c.sourceLevel == 0)
		if len(t0) != len(c.levels[0]) {
			imin, imax = t0.getRange(c.s.icmp)
		}
	}
	t1 = vt1.getOverlaps(t1, c.s.icmp, imin.ukey(), imax.ukey(), false)
	// Get entire range covered by compaction.
	amin, amax := append(t0, t1...).getRange(c.s.icmp)

	// See if we can grow the number of inputs in "sourceLevel" without
	// changing the number of "sourceLevel+1" files we pick up.
	if len(t1) > 0 {
		exp0 := vt0.getOverlaps(nil, c.s.icmp, amin.ukey(), amax.ukey(), c.sourceLevel == 0)
		if len(exp0) > len(t0) && t1.size()+exp0.size() < limit {
			xmin, xmax := exp0.getRange(c.s.icmp)
			exp1 := vt1.getOverlaps(nil, c.s.icmp, xmin.ukey(), xmax.ukey(), false)
			if len(exp1) == len(t1) {
				// 增选了 t0 上的 SSTable，并没有使的 t1 的 SSTable 被增选，即可以确定参与 compaction 的文件
				// 为了避免这种 expand 变得无穷无尽
				c.s.logf("table@compaction expanding L%d+L%d (F·%d S·%s)+(F·%d S·%s) -> (F·%d S·%s)+(F·%d S·%s)",
					c.sourceLevel, c.sourceLevel+1, len(t0), shortenb(t0.size()), len(t1), shortenb(t1.size()),
					len(exp0), shortenb(exp0.size()), len(exp1), shortenb(exp1.size()))
				imin, imax = xmin, xmax
				t0, t1 = exp0, exp1
				amin, amax = append(t0, t1...).getRange(c.s.icmp)
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == sourceLevel+1; grandparent == sourceLevel+2)
	if level := c.sourceLevel + 2; level < len(c.v.levels) {
		c.gp = c.v.levels[level].getOverlaps(c.gp, c.s.icmp, amin.ukey(), amax.ukey(), false)
	}

	c.levels[0], c.levels[1] = t0, t1
	c.imin, c.imax = imin, imax
}

// Check whether compaction is trivial.
func (c *compaction) trivial() bool {
	return len(c.levels[0]) == 1 && len(c.levels[1]) == 0 && c.gp.size() <= c.maxGPOverlaps
}

// 如果 ukey 只存在于 compaction 涉及到的两个 level，不在更高的 level 出现，返回 true，否则返回 false
// "baseLevel" 的意思在此
// 因为在 compaction 的过程中，从 iterator 出来的 ukey 必然是递增的，后面的 ukey 只会大于等于前面的 ukey
// 所以这个函数内部用到的 c.tPtrs[level] 是可以单调向前移动的
func (c *compaction) baseLevelForKey(ukey []byte) bool {
	// 从 sourceLevel+2 开始搜索
	for level := c.sourceLevel + 2; level < len(c.v.levels); level++ {
		tables := c.v.levels[level]
		for c.tPtrs[level] < len(tables) {
			t := tables[c.tPtrs[level]]
			if c.s.icmp.uCompare(ukey, t.imax.ukey()) <= 0 {
				// We've advanced far enough.
				if c.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					// Key falls in this file's range, so definitely not base level.
					// ukey 只是在这个文件的 range 内，其实并不一定代表这个文件一定包含这个 ukey ?
					// 感觉这里是为了判断的速度，于是保守一点，给出了 ukey 可能出现在更高层的结论
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
		// compaction 之后在 sourceLevel+1 之后产生的新文件 f 也不可以太大，如果过大了的话，f 会跟 sourceLevel+2 的文件有过多的交集
		// 那么将来当 f 需要做 compcation 的时候，下一层就会涉及到过多的文件，那时的 compaction 就过于 heavy 了
		// 所以这时要终止当前的 SSTable，开启下一个 SSTable
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
		DontFillCache: true, // 作 compaction 的时候读取的数据跟业务无关，不要填充 cache
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
			// tables 本身就是排好序的，所以 newIndexIterator 内部做二分是没问题的
			it := iterator.NewIndexedIterator(tables.newIndexIterator(c.s.tops, c.s.icmp, nil, ro), strict)
			its = append(its, it)
		}
	}

	return iterator.NewMergedIterator(its, c.s.icmp, strict)
}
