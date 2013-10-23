// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// table file
type tFile struct {
	file     storage.File
	seekLeft int32
	size     uint64
	min, max iKey
}

// test if key is after t
func (t *tFile) isAfter(key []byte, cmp comparer.BasicComparer) bool {
	return key != nil && cmp.Compare(key, t.max.ukey()) > 0
}

// test if key is before t
func (t *tFile) isBefore(key []byte, cmp comparer.BasicComparer) bool {
	return key != nil && cmp.Compare(key, t.min.ukey()) < 0
}

func (t *tFile) incrSeek() int32 {
	return atomic.AddInt32(&t.seekLeft, -1)
}

func newTFile(file storage.File, size uint64, min, max iKey) *tFile {
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
	return sum
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

func (p tFiles) getOverlaps(min, max []byte, r *tFiles, disjSorted bool, ucmp comparer.BasicComparer) {
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

func (p tFiles) newIndexIterator(tops *tOps, cmp *iComparer, ro *opt.ReadOptions) *tFilesIter {
	return &tFilesIter{
		tops: tops,
		cmp:  cmp,
		ro:   ro,
		tt:   p,
		pos:  -1,
	}
}

type tFilesIter struct {
	util.BasicReleaser
	tops *tOps
	cmp  *iComparer
	ro   *opt.ReadOptions
	tt   tFiles
	pos  int
}

func (i *tFilesIter) empty() bool {
	return len(i.tt) == 0
}

func (i *tFilesIter) Valid() bool {
	if i.pos < 0 || i.pos >= len(i.tt) {
		return false
	}
	return true
}

func (i *tFilesIter) First() bool {
	if i.empty() {
		return false
	}
	i.pos = 0
	return true
}

func (i *tFilesIter) Last() bool {
	if i.empty() {
		return false
	}
	i.pos = len(i.tt) - 1
	return true
}

func (i *tFilesIter) Seek(key []byte) bool {
	if i.empty() {
		return false
	}
	i.pos = i.tt.search(iKey(key), i.cmp)
	return i.pos < len(i.tt)
}

func (i *tFilesIter) Next() bool {
	if i.pos >= len(i.tt) {
		return false
	}
	i.pos++
	return i.pos < len(i.tt)
}

func (i *tFilesIter) Prev() bool {
	if i.pos < 0 {
		return false
	}
	i.pos--
	return i.pos >= 0
}

func (i *tFilesIter) Get() iterator.Iterator {
	if i.pos < 0 || i.pos >= len(i.tt) {
		return nil
	}
	return i.tops.newIterator(i.tt[i.pos], i.ro)
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
		return a.file.Num() < b.file.Num()
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
	return p[i].file.Num() > p[j].file.Num()
}

func (p tFileSorterNewest) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// table operations
type tOps struct {
	s       *session
	cache   cache.Cache
	cacheNS cache.Namespace
}

func newTableOps(s *session, cacheCap int) *tOps {
	c := cache.NewLRUCache(cacheCap)
	ns := c.GetNamespace(0)
	return &tOps{s, c, ns}
}

func (t *tOps) create() (*tWriter, error) {
	file := t.s.getTableFile(t.s.allocFileNum())
	fw, err := file.Create()
	if err != nil {
		return nil, err
	}
	return &tWriter{
		t:    t,
		file: file,
		w:    fw,
		tw:   table.NewWriter(fw, t.s.o),
	}, nil
}

func (t *tOps) createFrom(src iterator.Iterator) (f *tFile, n int, err error) {
	w, err := t.create()
	if err != nil {
		return f, n, err
	}

	defer func() {
		if err != nil {
			w.drop()
		}
	}()

	for src.Next() {
		err = w.add(src.Key(), src.Value())
		if err != nil {
			return
		}
	}
	err = src.Error()
	if err != nil {
		return
	}

	n = w.tw.EntriesLen()
	f, err = w.finish()
	return
}

func (t *tOps) lookup(f *tFile) (c cache.Object, err error) {
	num := f.file.Num()
	c, ok := t.cacheNS.Get(num, func() (ok bool, value interface{}, charge int, fin cache.SetFin) {
		var r storage.Reader
		r, err = f.file.Open()
		if err != nil {
			return
		}

		o := t.s.o

		var cacheNS cache.Namespace
		if bc := o.GetBlockCache(); bc != nil {
			cacheNS = bc.GetNamespace(num)
		}

		ok = true
		value = table.NewReader(r, int64(f.size), cacheNS, o)
		charge = 1
		fin = func() {
			r.Close()
		}
		return
	})
	if !ok && err == nil {
		err = ErrClosed
	}
	return
}

func (t *tOps) get(f *tFile, key []byte, ro *opt.ReadOptions) (rkey, rvalue []byte, err error) {
	c, err := t.lookup(f)
	if err != nil {
		return nil, nil, err
	}
	defer c.Release()
	return c.Value().(*table.Reader).Find(key, ro)
}

func (t *tOps) getApproximateOffset(f *tFile, key []byte) (offset uint64, err error) {
	c, err := t.lookup(f)
	if err != nil {
		return
	}
	_offset, err := c.Value().(*table.Reader).GetApproximateOffset(key)
	offset = uint64(_offset)
	c.Release()
	return
}

func (t *tOps) newIterator(f *tFile, ro *opt.ReadOptions) iterator.Iterator {
	c, err := t.lookup(f)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	iter := c.Value().(*table.Reader).NewIterator(ro)
	iter.SetReleaser(c)
	return iter
}

func (t *tOps) remove(f *tFile) {
	num := f.file.Num()
	t.cacheNS.Delete(num, func(exist bool) {
		f.file.Remove()
		if bc := t.s.o.GetBlockCache(); bc != nil {
			bc.GetNamespace(num).Zap(false)
		}
	})
}

func (t *tOps) close() {
	t.cache.Zap(true)
}

type tWriter struct {
	t *tOps

	file storage.File
	w    storage.Writer
	tw   *table.Writer

	notFirst    bool
	first, last []byte
}

func (w *tWriter) add(key, value []byte) error {
	if w.notFirst {
		w.last = append(w.last[:0], key...)
	} else {
		w.first = append(w.first[:0], key...)
		w.last = append(w.last[:0], key...)
		w.notFirst = true
	}
	return w.tw.Append(key, value)
}

func (w *tWriter) finish() (f *tFile, err error) {
	err = w.tw.Close()
	if err != nil {
		return
	}
	err = w.w.Sync()
	if err != nil {
		return
	}
	w.w.Close()
	f = newTFile(w.file, uint64(w.tw.BytesLen()), iKey(w.first), iKey(w.last))
	return
}

func (w *tWriter) drop() {
	w.w.Close()
	w.file.Remove()
	w.t.s.reuseFileNum(w.file.Num())
	w.w = nil
	w.file = nil
	w.tw = nil
}
