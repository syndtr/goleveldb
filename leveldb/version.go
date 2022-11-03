// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type tSet struct {
	level int
	table *tFile
}

// 保存每个层级的文件信息以及判断是否需要更新compaction相关的变量
type version struct {
	id int64 // unique monotonous increasing version id
	s  *session

	levels []tFiles

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed. These fields
	// are initialized by computeCompaction()
	cLevel int
	cScore float64

	cSeek unsafe.Pointer // 保存 tSet，需要被 compaction 的下一个 SSTable？

	closing  bool
	ref      int
	released bool
}

// newVersion creates a new version with an unique monotonous increasing id.
func newVersion(s *session) *version {
	id := atomic.AddInt64(&s.ntVersionID, 1)
	nv := &version{s: s, id: id - 1}
	return nv
}

func (v *version) incref() {
	if v.released {
		panic("already released")
	}

	v.ref++
	if v.ref == 1 {
		select {
		case v.s.refCh <- &vTask{vid: v.id, files: v.levels, created: time.Now()}:
			// We can use v.levels directly here since it is immutable.
		case <-v.s.closeC:
			v.s.log("reference loop already exist")
		}
	}
}

// 释放 version
// 当一个 version 引用计数减为 0 时，可以删除掉这个 version
func (v *version) releaseNB() {
	v.ref--
	if v.ref > 0 {
		return
	} else if v.ref < 0 {
		panic("negative version ref")
	}

	// 当 reference count 减少到 0 时，释放 version
	select {
	case v.s.relCh <- &vTask{vid: v.id, files: v.levels, created: time.Now()}:
		// We can use v.levels directly here since it is immutable.
	case <-v.s.closeC:
		v.s.log("reference loop already exist")
	}

	v.released = true
}

func (v *version) release() {
	v.s.vmu.Lock()
	v.releaseNB()
	v.s.vmu.Unlock()
}

// 因为 version 上有当前的 SSTable 信息，所以一些使用 SSTable 进行查询的方法定义在了 version 上
func (v *version) walkOverlapping(aux tFiles, ikey internalKey, f func(level int, t *tFile) bool, lf func(level int) bool) {
	ukey := ikey.ukey()

	// Aux level.
	if aux != nil {
		for _, t := range aux {
			// 注意：使用的是 user key 来判定是否和 SSTable 的 range 重叠
			if t.overlaps(v.s.icmp, ukey, ukey) {
				if !f(-1, t) {
					return
				}
			}
		}

		if lf != nil && !lf(-1) {
			return
		}
	}

	// Walk tables level-by-level.
	for level, tables := range v.levels {
		if len(tables) == 0 {
			continue
		}

		if level == 0 {
			// Level-0 files may overlap each other. Find all files that
			// overlap ukey.
			for _, t := range tables {
				if t.overlaps(v.s.icmp, ukey, ukey) {
					if !f(level, t) {
						return
					}
				}
			}
		} else {

			// Searches smallest index of tables whose its largest
			// key is after or equal with given key.
			// 找 SSTable 最小的 index，其 imax 大于等于 ikey，这个 SSTable 就是潜在的 table
			if i := tables.searchMax(v.s.icmp, ikey); i < len(tables) {
				t := tables[i]
				if v.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					if !f(level, t) {
						return
					}
				}
			}
		}

		if lf != nil && !lf(level) {
			// lf 内部会判断是否在 level-0 上已经找到，如果已经找到的话，这里会 return，就不会继续往下找了
			return
		}
	}
}

// 在 SSTable 中查找 ikey
// v 上说明了本次查询所有的 SSTable
func (v *version) get(aux tFiles, ikey internalKey, ro *opt.ReadOptions, noValue bool) (value []byte, tcomp bool, err error) {
	if v.closing {
		return nil, false, ErrClosed
	}

	ukey := ikey.ukey()
	sampleSeeks := !v.s.o.GetDisableSeeksCompaction() // 如果 seek 的时候 miss 过多，需要出发 compaction

	var (
		tset  *tSet
		tseek bool // 注：trigger seek compaction?

		// Level-0.
		zfound bool   // 标记 level-0 上已经找到了要找的 key，不可以继续往下找了，因为 level-0 上的数据肯定是最新的，如果继续往下找的话就可能找到旧的数据了
		zseq   uint64 // 用来和找到的 seq number 做比较，当遇到更大的 seq number 时更新结果，使用离 snapshot 最近的结果
		zkt    keyType
		zval   []byte
	)

	err = ErrNotFound

	// Since entries never hop across level, finding key/value
	// in smaller level make later levels irrelevant.
	v.walkOverlapping(aux, ikey,
		// 在一个 SSTable 内部查找，返回是否还需要继续到别的 SSTable 中搜索
		func(level int, t *tFile) bool {
			if sampleSeeks && level >= 0 && !tseek {
				if tset == nil {
					tset = &tSet{level, t}
				} else {
					// 当 tSet 不为 nil 时，tSet 保存的是之前一个 SSTable，之前的 SSTable 没有找到这个 key，所以要 consume 一次 seekNumber
					tseek = true
				}
			}

			var (
				fikey, fval []byte
				ferr        error
			)
			// 查询的是 internal key
			if noValue {
				fikey, ferr = v.s.tops.findKey(t, ikey, ro)
			} else {
				fikey, fval, ferr = v.s.tops.find(t, ikey, ro)
			}

			switch ferr {
			case nil:
			case ErrNotFound:
				// 在一个 SSTable 中没找到的话要继续寻找
				return true
			default:
				err = ferr
				// 查询过程中出错，就结束查询，不继续在别的 SSTable 中查询
				return false
			}

			if fukey, fseq, fkt, fkerr := parseInternalKey(fikey); fkerr == nil {
				if v.s.icmp.uCompare(ukey, fukey) == 0 {
					// Level <= 0 may overlaps each-other.
					if level <= 0 {
						if fseq >= zseq {
							// 使用更大的 seq number，越大的 seq number 越新，越靠近 snapshot
							zfound = true
							zseq = fseq
							zkt = fkt
							zval = fval
						}
					} else {
						switch fkt {
						case keyTypeVal:
							value = fval
							err = nil
						case keyTypeDel:
							// 这里因为处于 level 0 之下的 level，一旦发现了 ukey，就不用继续找了，所以不存在更新结果的情况，value 不用给 reset 成 nil
							// 只会赋值一次
						default:
							panic("leveldb: invalid internalKey type")
						}
						// 在 level 0 以下的层，一旦发现了就不用继续向下找了
						return false
					}
				}
			} else {
				err = fkerr
				// 查询过程中出错，就结束查询，不继续在别的 SSTable 中查询
				return false
			}

			return true
		}, func(level int) bool {
			// 遍历完 level-0 之后会调用这个函数，检查是否在 level-0 中已经发现了 key
			// 如果 zfound，就要返回结果了，不可以继续往下找了，不然的话可能会找到下层 level 上的、旧的数据
			if zfound {
				switch zkt {
				case keyTypeVal:
					value = zval
					err = nil
				case keyTypeDel:
				default:
					panic("leveldb: invalid internalKey type")
				}
				return false
			}

			// 对 level-0 之下的 level，调用是都返回 true，继续往下找
			// 所以 lf 这个函数只是对 zfound，“在 level-0 上是否找到”生效，用来终止向下的查找
			return true
		})

	// 感觉这里 consume seek 的条件有点鲁莽，如果这个 key 本来就是不存在的，目前看来也会 consume 一次 seek
	// 应该是最终这个 key 被找到了，然后对和它有 overlap 的 SSTable 都去 consume 一次 seek？
	if tseek && tset.table.consumeSeek() <= 0 {
		tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
	}

	return
}

func (v *version) sampleSeek(ikey internalKey) (tcomp bool) {
	var tset *tSet

	v.walkOverlapping(nil, ikey, func(level int, t *tFile) bool {
		if tset == nil {
			tset = &tSet{level, t}
			return true
		}
		if tset.table.consumeSeek() <= 0 {
			tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
		}
		return false
	}, nil)

	return
}

func (v *version) getIterators(slice *util.Range, ro *opt.ReadOptions) (its []iterator.Iterator) {
	strict := opt.GetStrict(v.s.o.Options, ro, opt.StrictReader)
	for level, tables := range v.levels {
		if level == 0 {
			// Merge all level zero files together since they may overlap.
			for _, t := range tables {
				its = append(its, v.s.tops.newIterator(t, slice, ro))
			}
		} else if len(tables) != 0 {
			its = append(its, iterator.NewIndexedIterator(tables.newIndexIterator(v.s.tops, v.s.icmp, slice, ro), strict))
		}
	}
	return
}

func (v *version) newStaging() *versionStaging {
	return &versionStaging{base: v}
}

// Spawn a new version based on this version.
func (v *version) spawn(r *sessionRecord, trivial bool) *version {
	staging := v.newStaging()
	staging.commit(r) // 将变更记录、收集到 version staging 中
	return staging.finish(trivial)
}

// 把 v 的所有 level 都添加到 r 中
func (v *version) fillRecord(r *sessionRecord) {
	for level, tables := range v.levels {
		for _, t := range tables {
			r.addTableFile(level, t)
		}
	}
}

func (v *version) tLen(level int) int {
	if level < len(v.levels) {
		return len(v.levels[level])
	}
	return 0
}

func (v *version) offsetOf(ikey internalKey) (n int64, err error) {
	for level, tables := range v.levels {
		for _, t := range tables {
			if v.s.icmp.Compare(t.imax, ikey) <= 0 {
				// Entire file is before "ikey", so just add the file size
				n += t.size
			} else if v.s.icmp.Compare(t.imin, ikey) > 0 {
				// Entire file is after "ikey", so ignore
				if level > 0 {
					// Files other than level 0 are sorted by meta->min, so
					// no further files in this level will contain data for
					// "ikey".
					break
				}
			} else {
				// "ikey" falls in the range for this table. Add the
				// approximate offset of "ikey" within the table.
				if m, err := v.s.tops.offsetOf(t, ikey); err == nil {
					n += m
				} else {
					return 0, err
				}
			}
		}
	}

	return
}

// maxLevel 通常只用于 testing，prod 使用中取 0
func (v *version) pickMemdbLevel(umin, umax []byte, maxLevel int) (level int) {
	if maxLevel > 0 {
		if len(v.levels) == 0 {
			return maxLevel
		}
		if !v.levels[0].overlaps(v.s.icmp, umin, umax, true) {
			var overlaps tFiles
			for ; level < maxLevel; level++ {
				if pLevel := level + 1; pLevel >= len(v.levels) {
					return maxLevel
				} else if v.levels[pLevel].overlaps(v.s.icmp, umin, umax, false) {
					break
				}
				if gpLevel := level + 2; gpLevel < len(v.levels) {
					overlaps = v.levels[gpLevel].getOverlaps(overlaps, v.s.icmp, umin, umax, false)
					if overlaps.size() > int64(v.s.o.GetCompactionGPOverlaps(level)) {
						break
					}
				}
			}
		}
	}
	return
}

func (v *version) computeCompaction() {
	// Precomputed best level for next compaction
	bestLevel := int(-1)
	bestScore := float64(-1)

	statFiles := make([]int, len(v.levels))
	statSizes := make([]string, len(v.levels))
	statScore := make([]string, len(v.levels))
	statTotSize := int64(0)

	for level, tables := range v.levels {
		var score float64
		size := tables.size()
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compaction.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = float64(len(tables)) / float64(v.s.o.GetCompactionL0Trigger())
		} else {
			score = float64(size) / float64(v.s.o.GetCompactionTotalSize(level))
		}

		if score > bestScore {
			bestLevel = level
			bestScore = score
		}

		statFiles[level] = len(tables)
		statSizes[level] = shortenb(size)
		statScore[level] = fmt.Sprintf("%.2f", score)
		statTotSize += size
	}

	v.cLevel = bestLevel
	v.cScore = bestScore

	v.s.logf("version@stat F·%v S·%s%v Sc·%v", statFiles, shortenb(statTotSize), statSizes, statScore)
}

func (v *version) needCompaction() bool {
	return v.cScore >= 1 || atomic.LoadPointer(&v.cSeek) != nil
}

type tablesScratch struct {
	added   map[int64]atRecord // key: file num
	deleted map[int64]struct{} // key: file num
}

// 描述下一个 version 相比于 base version 的变化，增加了哪些 SSTable，删掉了哪些 SSTable
type versionStaging struct {
	base   *version
	levels []tablesScratch
}

// 之所以叫 "Scratch"，是因为我们需要收集 rec 形成这个 version staging，收集的过程像是 from the scratch
func (p *versionStaging) getScratch(level int) *tablesScratch {
	if level >= len(p.levels) {
		newLevels := make([]tablesScratch, level+1)
		copy(newLevels, p.levels)
		p.levels = newLevels
	}
	return &(p.levels[level])
}

func (p *versionStaging) commit(r *sessionRecord) {
	// Deleted tables.
	for _, r := range r.deletedTables {
		scratch := p.getScratch(r.level)
		if r.level < len(p.base.levels) && len(p.base.levels[r.level]) > 0 {
			// 只有 base version 真的有要删的这个 level，这里做一个简单的预 check
			if scratch.deleted == nil {
				scratch.deleted = make(map[int64]struct{})
			}
			scratch.deleted[r.num] = struct{}{}
		}
		if scratch.added != nil {
			// 如果是要删的，就不能继续在 add 里面了，预处理
			delete(scratch.added, r.num)
		}
	}

	// New tables.
	for _, r := range r.addedTables {
		scratch := p.getScratch(r.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[r.num] = r
		if scratch.deleted != nil {
			delete(scratch.deleted, r.num)
		}
	}
}

// 由 version staging 构建一个新的 version
func (p *versionStaging) finish(trivial bool) *version {
	// Build new version.
	nv := newVersion(p.base.s)
	numLevel := len(p.levels)
	if len(p.base.levels) > numLevel {
		numLevel = len(p.base.levels)
	}
	nv.levels = make([]tFiles, numLevel)
	for level := 0; level < numLevel; level++ {
		// 逐个 level 地 delete、add SSTables
		var baseTabels tFiles
		if level < len(p.base.levels) {
			baseTabels = p.base.levels[level]
		}

		if level < len(p.levels) {
			scratch := p.levels[level]

			// Short circuit if there is no change at all.
			if len(scratch.added) == 0 && len(scratch.deleted) == 0 {
				nv.levels[level] = baseTabels
				continue
			}

			var nt tFiles
			// Prealloc list if possible.
			if n := len(baseTabels) + len(scratch.added) - len(scratch.deleted); n > 0 {
				nt = make(tFiles, 0, n)
			}

			// Base tables.
			for _, t := range baseTabels {
				if _, ok := scratch.deleted[t.fd.Num]; ok {
					continue
				}
				if _, ok := scratch.added[t.fd.Num]; ok {
					continue
				}
				nt = append(nt, t)
			}

			// Avoid resort if only files in this level are deleted
			if len(scratch.added) == 0 {
				nv.levels[level] = nt
				continue
			}

			// For normal table compaction, one compaction will only involve two levels
			// of files. And the new files generated after merging the source level and
			// source+1 level related files can be inserted as a whole into source+1 level
			// without any overlap with the other source+1 files.
			//
			// When the amount of data maintained by leveldb is large, the number of files
			// per level will be very large. While qsort is very inefficient for sorting
			// already ordered arrays. Therefore, for the normal table compaction, we use
			// binary search here to find the insert index to insert a batch of new added
			// files directly instead of using qsort.
			if trivial && len(scratch.added) > 0 {
				added := make(tFiles, 0, len(scratch.added))
				for _, r := range scratch.added {
					added = append(added, tableFileFromRecord(r))
				}

				if level == 0 {
					// 在 level=0 上，added 按照 file number 降序排序
					// level-0 上的 SSTable 可能有 range 上的重叠，而且没有顺序性
					added.sortByNum()
					index := nt.searchNumLess(added[len(added)-1].fd.Num)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				} else {
					// 其他 level 上，SSTable 是按照 imin 排序的，imin 相同的话按照 num 升序排序；
					added.sortByKey(p.base.s.icmp) // 按照 imin 升序排序
					_, amax := added.getRange(p.base.s.icmp)
					index := nt.searchMin(p.base.s.icmp, amax) // 找 nt 中第一个 imin 大于等于 amax 的 SSTable，于是 added 可以被 insert 到 index 前面
					nt = append(nt[:index], append(added, nt[index:]...)...)
				}
				nv.levels[level] = nt
				continue
			}

			// New tables.
			for _, r := range scratch.added {
				nt = append(nt, tableFileFromRecord(r))
			}

			if len(nt) != 0 {
				// Sort tables.
				if level == 0 {
					nt.sortByNum()
				} else {
					nt.sortByKey(p.base.s.icmp)
				}

				nv.levels[level] = nt
			}
		} else {
			nv.levels[level] = baseTabels
		}
	}

	// Trim levels.
	n := len(nv.levels)
	for ; n > 0 && nv.levels[n-1] == nil; n-- {
	}
	nv.levels = nv.levels[:n]

	// Compute compaction score for new version.
	nv.computeCompaction()

	return nv
}

type versionReleaser struct {
	v    *version
	once bool
}

func (vr *versionReleaser) Release() {
	v := vr.v
	v.s.vmu.Lock()
	if !vr.once {
		v.releaseNB()
		vr.once = true
	}
	v.s.vmu.Unlock()
}
