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

package table

import (
	"leveldb"
	"leveldb/block"
	"leveldb/descriptor"
	"runtime"
)

type Reader struct {
	r descriptor.Reader
	o leveldb.OptionsInterface

	meta   *block.Reader
	index  *block.Reader
	filter *block.FilterReader

	dataEnd uint64
	cache   leveldb.CacheNamespace
}

func NewReader(r descriptor.Reader, size uint64, o leveldb.OptionsInterface, cache leveldb.CacheNamespace) (t *Reader, err error) {
	mi, ii, err := readFooter(r, size)
	if err != nil {
		return
	}

	bb, err := ii.readAll(r, true)
	if err != nil {
		return
	}
	var index *block.Reader
	index, err = block.NewReader(bb)
	if err != nil {
		return
	}

	dataEnd := mi.offset

	var filter *block.FilterReader
	filterPolicy := o.GetFilter()
	if filterPolicy != nil {
		bb, err = mi.readAll(r, true)
		if err != nil {
			return
		}
		var meta *block.Reader
		meta, err = block.NewReader(bb)
		if err != nil {
			return
		}
		iter := meta.NewIterator(o.GetComparer())
		key := "filter." + filterPolicy.Name()
		if iter.Seek([]byte(key)) && string(iter.Key()) == key {
			fh := new(bInfo)
			_, err = fh.decodeFrom(iter.Value())
			if err != nil {
				return
			}
			dataEnd = fh.offset
			bb, err = fh.readAll(r, true)
			if err != nil {
				return
			}
			filter, err = block.NewFilterReader(bb, filterPolicy)
			if err != nil {
				return
			}
		} else {
			err = iter.Error()
			if err != nil {
				return
			}
		}
	}

	t = &Reader{
		r:       r,
		o:       o,
		index:   index,
		filter:  filter,
		dataEnd: dataEnd,
		cache:   cache,
	}
	return
}

func (t *Reader) NewIterator(ro leveldb.ReadOptionsInterface) leveldb.Iterator {
	index_iter := &indexIter{t: t, ro: ro}
	t.index.InitIterator(&index_iter.Iterator, t.o.GetComparer())
	return leveldb.NewIndexedIterator(index_iter)
}

func (t *Reader) Get(key []byte, ro leveldb.ReadOptionsInterface) (rkey, rvalue []byte, err error) {
	index_iter := t.index.NewIterator(t.o.GetComparer())
	if !index_iter.Seek(key) {
		err = index_iter.Error()
		if err == nil {
			err = leveldb.ErrNotFound
		}
		return
	}

	bi := new(bInfo)
	_, err = bi.decodeFrom(index_iter.Value())
	if err != nil {
		return
	}
	if t.filter == nil || t.filter.KeyMayMatch(uint(bi.offset), key) {
		var iter leveldb.Iterator
		iter, err = t.getBlock(bi, ro)
		if !iter.Seek(key) {
			err = iter.Error()
			if err == nil {
				err = leveldb.ErrNotFound
			}
			return
		}
		rkey, rvalue = iter.Key(), iter.Value()
	}
	return
}

func (t *Reader) ApproximateOffsetOf(key []byte) uint64 {
	index_iter := t.index.NewIterator(t.o.GetComparer())
	if index_iter.Seek(key) {
		bi := new(bInfo)
		_, err := bi.decodeFrom(index_iter.Value())
		if err == nil {
			return bi.offset
		}
	}
	return t.dataEnd
}

func (t *Reader) getBlock(bi *bInfo, ro leveldb.ReadOptionsInterface) (iter leveldb.Iterator, err error) {
	var b *block.Reader
	newBlock := func() {
		bb, err := bi.readAll(t.r, ro.HasFlag(leveldb.RFVerifyChecksums))
		if err != nil {
			return
		}
		b, err = block.NewReader(bb)
	}

	var cacheObj leveldb.CacheObject
	if t.cache != nil {
		var ok bool
		cacheObj, ok = t.cache.Get(bi.offset)
		if ok {
			b = cacheObj.Value().(*block.Reader)
		} else {
			newBlock()
			if err != nil {
				return
			}
			cacheObj = t.cache.Set(bi.offset, b, int(bi.size), nil)
		}
	} else {
		newBlock()
		if err != nil {
			return
		}
	}

	biter := b.NewIterator(t.o.GetComparer())
	if cacheObj != nil {
		runtime.SetFinalizer(biter, func(x *block.Iterator) {
			cacheObj.Release()
		})
	}
	return biter, nil
}

type indexIter struct {
	block.Iterator

	t  *Reader
	ro leveldb.ReadOptionsInterface
}

func (i *indexIter) Get() (iter leveldb.Iterator, err error) {
	bi := new(bInfo)
	_, err = bi.decodeFrom(i.Value())
	if err != nil {
		return
	}

	return i.t.getBlock(bi, i.ro)
}
