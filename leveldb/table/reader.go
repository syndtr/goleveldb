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
	"encoding/binary"
	"leveldb"
	"leveldb/block"
	"leveldb/descriptor"
	"runtime"
)

type Reader struct {
	r descriptor.Reader
	o *leveldb.Options

	meta   *block.Reader
	index  *block.Reader
	filter *block.FilterReader

	dataEnd uint64
	cacheId uint64
}

func NewReader(r descriptor.Reader, size uint64, o *leveldb.Options, cacheId uint64) (t *Reader, err error) {
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
	filterPolicy := o.GetFilterPolicy()
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
		iter := meta.NewIterator(o.GetComparator())
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
		cacheId: cacheId,
	}
	return
}

func (t *Reader) NewIterator(o *leveldb.ReadOptions) leveldb.Iterator {
	index_iter := t.index.NewIterator(t.o.GetComparator())
	return leveldb.NewTwoLevelIterator(index_iter, &bGet{t: t, o: o})
}

func (t *Reader) Get(key []byte, o *leveldb.ReadOptions) (rkey, rvalue []byte, err error) {
	index_iter := t.index.NewIterator(t.o.GetComparator())
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
		iter, err = t.getBlock(bi, o)
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
	index_iter := t.index.NewIterator(t.o.GetComparator())
	if index_iter.Seek(key) {
		bi := new(bInfo)
		_, err := bi.decodeFrom(index_iter.Value())
		if err == nil {
			return bi.offset
		}
	}
	return t.dataEnd
}

func (t *Reader) getBlock(bi *bInfo, o *leveldb.ReadOptions) (iter leveldb.Iterator, err error) {
	var b *block.Reader
	newBlock := func() {
		bb, err := bi.readAll(t.r, o.HasFlag(leveldb.RFVerifyChecksums))
		if err != nil {
			return
		}
		b, err = block.NewReader(bb)
	}

	var cacheObj leveldb.CacheObject
	cache := t.o.GetBlockCache()
	if cache != nil {
		cacheKey := make([]byte, 16)
		binary.LittleEndian.PutUint64(cacheKey, t.cacheId)
		binary.LittleEndian.PutUint64(cacheKey[8:], bi.offset)

		var ok bool
		cacheObj, ok = cache.Get(cacheKey)
		if ok {
			b = cacheObj.Value().(*block.Reader)
		} else {
			newBlock()
			if err != nil {
				return
			}
			cacheObj = cache.Set(cacheKey, b, int(bi.size))
		}
	} else {
		newBlock()
		if err != nil {
			return
		}
	}

	iter = b.NewIterator(t.o.GetComparator())
	if cacheObj != nil {
		if biter, ok := iter.(*block.Iterator); ok {
			setCacheFinalizer(biter, cacheObj)
		} else {
			cacheObj.Release()
		}
	}
	return
}

type bGet struct {
	t *Reader
	o *leveldb.ReadOptions
}

func (g *bGet) Get(value []byte) (iter leveldb.Iterator, err error) {
	bi := new(bInfo)
	_, err = bi.decodeFrom(value)
	if err != nil {
		return
	}

	return g.t.getBlock(bi, g.o)
}

func setCacheFinalizer(x *block.Iterator, cache leveldb.CacheObject) {
	runtime.SetFinalizer(x, func(x *block.Iterator) {
		cache.Release()
	})
}
