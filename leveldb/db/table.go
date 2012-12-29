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
	"encoding/binary"
	"leveldb"
	"leveldb/descriptor"
	"leveldb/table"
	"runtime"
	"sort"
)

// table file
type tFile struct {
	file     descriptor.File
	seekLeft int32
	size     uint64
	min, max iKey
}

// test if key is after t
func (t *tFile) isAfter(key []byte, cmp leveldb.BasicComparer) bool {
	return key != nil && cmp.Compare(key, t.max.ukey()) > 0
}

// test if key is before t
func (t *tFile) isBefore(key []byte, cmp leveldb.BasicComparer) bool {
	return key != nil && cmp.Compare(key, t.min.ukey()) < 0
}

func newTFile(file descriptor.File, size uint64, min, max iKey) *tFile {
	f := &tFile{
		file: file,
		size: size,
		min:  min,
		max:  max,
	}

	// We arrange to automatically compact this file after
	// a certain number of seeks.  Let's assume:
	//   (1) One seek costs 10ms
	//   (2) Writing or reading 1MB costs 10ms (100MB/s)
	//   (3) A compaction of 1MB does 25MB of IO:
	//         1MB read from this level
	//         10-12MB read from next level (boundaries may be misaligned)
	//         10-12MB written to next level
	// This implies that 25 seeks cost the same as the compaction
	// of 1MB of data.  I.e., one seek costs approximately the
	// same as the compaction of 40KB of data.  We are a little
	// conservative and allow approximately one seek for every 16KB
	// of data before triggering a compaction.
	f.seekLeft = int32(size / 16384)
	if f.seekLeft < 100 {
		f.seekLeft = 100
	}

	return f
}

// table files
type tFiles []*tFile

func (p tFiles) size() (sum uint64) {
	for _, f := range p {
		sum += f.size
	}
	return
}

func (p tFiles) sort(s tFileSorter) {
	sort.Sort(s.getSorter(p))
}

func (p tFiles) search(key iKey, cmp *iComparer) int {
	return sort.Search(len(p), func(i int) bool {
		return cmp.Compare(p[i].max, key) >= 0
	})
}

func (p tFiles) isOverlaps(min, max []byte, disjSorted bool, cmp *iComparer) bool {
	ucmp := cmp.cmp

	if !disjSorted {
		// Need to check against all files
		for _, t := range p {
			if !t.isAfter(min, ucmp) && !t.isBefore(max, ucmp) {
				return true
			}
		}
		return false
	}

	var idx int
	if len(min) > 0 {
		// Find the earliest possible internal key for min
		idx = p.search(newIKey(min, kMaxSeq, tSeek), cmp)
	}

	if idx >= len(p) {
		// beginning of range is after all files, so no overlap
		return false
	}
	return !p[idx].isBefore(max, ucmp)
}

func (p tFiles) getOverlaps(min, max []byte, r *tFiles, disjSorted bool, ucmp leveldb.BasicComparer) {
	for i := 0; i < len(p); {
		t := p[i]
		i++
		if t.isAfter(min, ucmp) || t.isBefore(max, ucmp) {
			continue
		}

		*r = append(*r, t)
		if !disjSorted {
			// Level-0 files may overlap each other.  So check if the newly
			// added file has expanded the range.  If so, restart search.
			if min != nil && ucmp.Compare(t.min.ukey(), min) < 0 {
				min = t.min.ukey()
				*r = nil
				i = 0
			} else if max != nil && ucmp.Compare(t.max.ukey(), max) > 0 {
				max = t.max.ukey()
				*r = nil
				i = 0
			}
		}
	}

	return
}

func (p tFiles) getRange(cmp *iComparer) (min, max iKey) {
	for i, t := range p {
		if i == 0 {
			min, max = t.min, t.max
			continue
		}
		if cmp.Compare(t.min, min) < 0 {
			min = t.min
		}
		if cmp.Compare(t.max, max) > 0 {
			max = t.max
		}
	}
	return
}

func (p tFiles) newIndexIterator(tops *tOps, cmp *iComparer, ro *leveldb.ReadOptions) *tFilesIter {
	return &tFilesIter{tops, cmp, ro, p, -1}
}

type tFilesIter struct {
	tops *tOps
	cmp  *iComparer
	ro   *leveldb.ReadOptions
	tt   tFiles
	pos  int
}

func (i *tFilesIter) Empty() bool {
	return len(i.tt) == 0
}

func (i *tFilesIter) Valid() bool {
	if i.pos < 0 || i.pos >= len(i.tt) {
		return false
	}
	return true
}

func (i *tFilesIter) First() bool {
	if i.Empty() {
		return false
	}
	i.pos = 0
	return true
}

func (i *tFilesIter) Last() bool {
	if i.Empty() {
		return false
	}
	i.pos = len(i.tt) - 1
	return true
}

func (i *tFilesIter) Seek(key []byte) bool {
	if i.Empty() {
		return false
	}
	i.pos = i.tt.search(iKey(key), i.cmp)
	return i.pos < len(i.tt)
}

func (i *tFilesIter) Next() bool {
	if i.Empty() || i.pos >= len(i.tt) {
		return false
	}
	i.pos++
	return true
}

func (i *tFilesIter) Prev() bool {
	if i.pos < 0 {
		return false
	}
	i.pos--
	return true
}

func (i *tFilesIter) Get() (iter leveldb.Iterator, err error) {
	if i.pos < 0 || i.pos >= len(i.tt) {
		return &leveldb.EmptyIterator{}, nil
	}
	return i.tops.newIterator(i.tt[i.pos], i.ro), nil
}

func (i *tFilesIter) Error() error {
	return nil
}

// table file sorter interface
type tFileSorter interface {
	getSorter(ff tFiles) sort.Interface
}

// sort table files by key
type tFileSorterKey struct {
	cmp *iComparer
	ff  tFiles
}

func (p *tFileSorterKey) getSorter(ff tFiles) sort.Interface {
	p.ff = ff
	return p
}

func (p *tFileSorterKey) Len() int {
	return len(p.ff)
}

func (p *tFileSorterKey) Less(i, j int) bool {
	a, b := p.ff[i], p.ff[j]
	n := p.cmp.Compare(a.min, b.min)
	if n == 0 {
		return a.file.Number() < b.file.Number()
	}
	return n < 0
}

func (p *tFileSorterKey) Swap(i, j int) {
	p.ff[i], p.ff[j] = p.ff[j], p.ff[i]
}

// sort table files by number
type tFileSorterNewest tFiles

func (p tFileSorterNewest) getSorter(ff tFiles) sort.Interface {
	return tFileSorterNewest(ff)
}

func (p tFileSorterNewest) Len() int { return len(p) }

func (p tFileSorterNewest) Less(i, j int) bool {
	return p[i].file.Number() > p[j].file.Number()
}

func (p tFileSorterNewest) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// table cache
type tOps struct {
	s     *session
	cache leveldb.Cache
}

func newTableOps(s *session, cacheCap int) *tOps {
	return &tOps{s, leveldb.NewLRUCache(cacheCap, nil)}
}

func (t *tOps) create() (w *tWriter, err error) {
	file := t.s.getTableFile(t.s.allocFileNum())
	fw, err := file.Create()
	if err != nil {
		return
	}
	return &tWriter{
		t:    t,
		file: file,
		w:    fw,
		tw:   table.NewWriter(fw, t.s.opt),
	}, nil
}

func (t *tOps) createFrom(src leveldb.Iterator) (f *tFile, n int, err error) {
	w, err := t.create()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			w.drop()
		}
	}()

	for src.Next() {
		err = w.add(src.Key(), src.Value())
		if err != nil {
			w.drop()
			return
		}
	}
	err = src.Error()
	if err != nil {
		return
	}

	n = w.tw.Len()
	f, err = w.finish()
	return
}

func (t *tOps) newIterator(f *tFile, ro *leveldb.ReadOptions) leveldb.Iterator {
	c, err := t.lookup(f)
	if err != nil {
		return &leveldb.EmptyIterator{err}
	}
	iter := c.Value().(*table.Reader).NewIterator(ro)
	if p, ok := iter.(*leveldb.IndexedIterator); ok {
		setTableIterFinalizer(p, c)
	} else {
		panic("not reached")
	}
	return iter
}

func (t *tOps) get(f *tFile, key []byte, ro *leveldb.ReadOptions) (rkey, rvalue []byte, err error) {
	c, err := t.lookup(f)
	if err != nil {
		return
	}
	defer c.Release()
	return c.Value().(*table.Reader).Get(key, ro)
}

func (t *tOps) approximateOffsetOf(f *tFile, key []byte) (n uint64, err error) {
	c, err := t.lookup(f)
	if err != nil {
		return
	}
	defer c.Release()
	n = c.Value().(*table.Reader).ApproximateOffsetOf(key)
	return
}

func (t *tOps) remove(f *tFile) {
	cacheKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(cacheKey, f.file.Number())

	t.cache.Delete(cacheKey, func() {
		f.file.Remove()
	})
}

func (t *tOps) purgeCache() {
	t.cache.Purge(nil)
}

func (t *tOps) lookup(f *tFile) (c leveldb.CacheObject, err error) {
	cacheKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(cacheKey, f.file.Number())

	var ok bool
	c, ok = t.cache.Get(cacheKey)
	if !ok {
		var r descriptor.Reader
		r, err = f.file.Open()
		if err != nil {
			return
		}
		var p *table.Reader
		p, err = table.NewReader(r, f.size, t.s.opt, f.file.Number())
		if err != nil {
			return
		}
		c = t.cache.Set(cacheKey, p, 1, func() {
			r.Close()
		})
	}
	return
}

func setTableIterFinalizer(x *leveldb.IndexedIterator, cache leveldb.CacheObject) {
	runtime.SetFinalizer(x, func(x *leveldb.IndexedIterator) {
		cache.Release()
	})
}

type tWriter struct {
	t *tOps

	file descriptor.File
	w    descriptor.Writer
	tw   *table.Writer

	notFirst    bool
	first, last []byte
}

func (w *tWriter) add(key, value []byte) error {
	if w.notFirst {
		w.last = key
	} else {
		w.first = key
		w.last = key
		w.notFirst = true
	}
	return w.tw.Add(key, value)
}

func (w *tWriter) finish() (t *tFile, err error) {
	defer func() {
		w.w.Close()
	}()

	err = w.tw.Finish()
	if err != nil {
		return
	}
	err = w.w.Sync()
	if err != nil {
		return
	}
	t = newTFile(w.file, uint64(w.tw.Size()), iKey(w.first), iKey(w.last))
	return
}

func (w *tWriter) drop() {
	w.w.Close()
	w.file.Remove()
	w.t.s.reuseFileNum(w.file.Number())
	w.w = nil
	w.file = nil
	w.tw = nil
}
