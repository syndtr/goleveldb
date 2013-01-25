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
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var levelMaxSize [kNumLevels]float64

func init() {
	// Precompute max size of each level
	for level := range levelMaxSize {
		res := float64(10 * 1048576)
		for n := level; n > 1; n-- {
			res *= 10
		}
		levelMaxSize[level] = res
	}
}

type tSet struct {
	level int
	table *tFile
}

type version struct {
	s *session

	tables [kNumLevels]tFiles

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed.  These fields
	// are initialized by ComputeCompaction()
	cLevel int
	cScore float64

	cSeek unsafe.Pointer

	next *version
}

func (v *version) purge() {
	if v.next == nil {
		return
	}

	s := v.s

	next := v.next
	v.next = nil

	tables := make(map[uint64]struct{})
	for _, tt := range next.tables {
		for _, t := range tt {
			tables[t.file.Num()] = struct{}{}
		}
	}

	for _, tt := range v.tables {
		for _, t := range tt {
			if _, ok := tables[t.file.Num()]; !ok {
				s.tops.remove(t)
			}
		}
	}

	next.setfin()
}

func (v *version) setfin() {
	runtime.SetFinalizer(v, func(x *version) {
		go x.purge()
	})
}

func (v *version) get(key iKey, ro *opt.ReadOptions) (value []byte, cstate bool, err error) {
	s := v.s
	icmp := s.cmp
	ucmp := icmp.cmp

	ukey := key.ukey()

	var tset *tSet
	tseek := true

	// We can search level-by-level since entries never hop across
	// levels. Therefore we are guaranteed that if we find data
	// in an smaller level, later levels are irrelevant.
	for level, ts := range v.tables {
		if len(ts) == 0 {
			continue
		}

		if level == 0 {
			// Level-0 files may overlap each other. Find all files that
			// overlap user_key and process them in order from newest to
			var tmp tFiles
			for _, t := range ts {
				if ucmp.Compare(ukey, t.min.ukey()) >= 0 &&
					ucmp.Compare(ukey, t.max.ukey()) <= 0 {
					tmp = append(tmp, t)
				}
			}

			if len(tmp) == 0 {
				continue
			}

			tmp.sort(tFileSorterNewest(nil))
			ts = tmp
		} else {
			i := ts.search(key, icmp)
			if i >= len(ts) || ucmp.Compare(ukey, ts[i].min.ukey()) < 0 {
				continue
			}

			ts = ts[i : i+1]
		}

		for _, t := range ts {
			if tseek {
				if tset == nil {
					tset = &tSet{level, t}
				} else if tset.table.incrSeek() <= 0 {
					cstate = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
					tseek = false
				}
			}

			var _rkey, rval []byte
			_rkey, rval, err = s.tops.get(t, key, ro)
			if err == errors.ErrNotFound {
				continue
			} else if err != nil {
				return
			}

			rkey := iKey(_rkey)
			if _, t, ok := rkey.parseNum(); ok {
				if ucmp.Compare(ukey, rkey.ukey()) == 0 {
					switch t {
					case tVal:
						value = rval
					case tDel:
						err = errors.ErrNotFound
					default:
						panic("not reached")
					}
					return
				}
			} else {
				err = errors.ErrCorrupt("internal key corrupted")
				return
			}
		}
	}

	err = errors.ErrNotFound
	return
}

func (v *version) getIterators(ro *opt.ReadOptions) (its []iter.Iterator) {
	s := v.s
	icmp := s.cmp

	// Merge all level zero files together since they may overlap
	for _, t := range v.tables[0] {
		it := s.tops.newIterator(t, ro)
		its = append(its, it)
	}

	for _, tt := range v.tables[1:] {
		if len(tt) == 0 {
			continue
		}

		it := iter.NewIndexedIterator(tt.newIndexIterator(s.tops, icmp, ro))
		its = append(its, it)
	}

	return
}

func (v *version) newStaging() *versionStaging {
	return &versionStaging{base: v}
}

// Spawn a new version based on this version.
func (v *version) spawn(r *sessionRecord) *version {
	staging := v.newStaging()
	staging.commit(r)
	return staging.finish()
}

func (v *version) fillRecord(r *sessionRecord) {
	for level, ts := range v.tables {
		for _, t := range ts {
			r.addTableFile(level, t)
		}
	}
}

func (v *version) tLen(level int) int {
	return len(v.tables[level])
}

func (v *version) approximateOffsetOf(key iKey) (n uint64, err error) {
	icmp := v.s.cmp
	tops := v.s.tops

	for level, tt := range v.tables {
		for _, t := range tt {
			if icmp.Compare(t.max, key) <= 0 {
				// Entire file is before "key", so just add the file size
				n += t.size
			} else if icmp.Compare(t.min, key) > 0 {
				// Entire file is after "key", so ignore
				if level > 0 {
					// Files other than level 0 are sorted by meta->min, so
					// no further files in this level will contain data for
					// "key".
					break
				}
			} else {
				// "key" falls in the range for this table.  Add the
				// approximate offset of "key" within the table.
				var nn uint64
				nn, err = tops.approximateOffsetOf(t, key)
				if err != nil {
					return 0, err
				}
				n += nn
			}
		}
	}

	return
}

func (v *version) pickLevel(min, max []byte) (level int) {
	icmp := v.s.cmp
	ucmp := icmp.cmp

	if !v.tables[0].isOverlaps(min, max, false, icmp) {
		var r tFiles
		for ; level < kMaxMemCompactLevel; level++ {
			if v.tables[level+1].isOverlaps(min, max, true, icmp) {
				break
			}
			v.tables[level+2].getOverlaps(min, max, &r, true, ucmp)
			if r.size() > kMaxGrandParentOverlapBytes {
				break
			}
		}
	}

	return
}

func (v *version) computeCompaction() {
	// Precomputed best level for next compaction
	var bestLevel int = -1
	var bestScore float64 = -1

	for level, ff := range v.tables {
		var score float64
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compactions.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = float64(len(ff)) / kL0_CompactionTrigger
		} else {
			score = float64(ff.size()) / levelMaxSize[level]
		}

		if score > bestScore {
			bestLevel = level
			bestScore = score
		}
	}

	v.cLevel = bestLevel
	v.cScore = bestScore
}

func (v *version) needCompaction() bool {
	return v.cScore >= 1 || atomic.LoadPointer(&v.cSeek) != nil
}

type versionStaging struct {
	base   *version
	tables [kNumLevels]struct {
		added   map[uint64]ntRecord
		deleted map[uint64]struct{}
	}
}

func (p *versionStaging) commit(r *sessionRecord) {
	btt := p.base.tables

	// deleted tables
	for _, tr := range r.deletedTables {
		tm := &(p.tables[tr.level])

		bt := btt[tr.level]
		if len(bt) > 0 {
			if tm.deleted == nil {
				tm.deleted = make(map[uint64]struct{})
			}
			tm.deleted[tr.num] = struct{}{}
		}

		if tm.added != nil {
			delete(tm.added, tr.num)
		}
	}

	// new tables
	for _, tr := range r.newTables {
		tm := &(p.tables[tr.level])

		if tm.added == nil {
			tm.added = make(map[uint64]ntRecord)
		}
		tm.added[tr.num] = tr

		if tm.deleted != nil {
			delete(tm.deleted, tr.num)
		}
	}
}

func (p *versionStaging) finish() *version {
	s := p.base.s
	btt := p.base.tables

	// build new version
	nv := &version{s: s}
	sorter := &tFileSorterKey{cmp: s.cmp}
	for level, tm := range p.tables {
		bt := btt[level]

		n := len(bt) + len(tm.added) - len(tm.deleted)
		if n < 0 {
			n = 0
		}
		nt := make(tFiles, 0, n)

		// base tables
		for _, t := range bt {
			if _, ok := tm.deleted[t.file.Num()]; ok {
				continue
			}
			if _, ok := tm.added[t.file.Num()]; ok {
				continue
			}
			nt = append(nt, t)
		}

		// new tables
		for _, tr := range tm.added {
			nt = append(nt, tr.makeFile(s))
		}

		// sort tables
		nt.sort(sorter)
		nv.tables[level] = nt
	}

	// compute compaction score for new version
	nv.computeCompaction()

	return nv
}
