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
	"bytes"
	"encoding/binary"
	"runtime"
	"leveldb"
)

type Table struct {
	r       leveldb.Reader
	o       *leveldb.Options

	meta    *block
	index   *block

	filter  *filterBlock

	dataEnd uint64
	cacheId uint64
}

func NewTable(r leveldb.Reader, size uint64, o *leveldb.Options, cacheId uint64) (t *Table, err error) {
	var metaHandle, indexHandle *blockHandle
	metaHandle, indexHandle, err = readFooter(r, size)
	if err != nil {
		return
	}

	var index *block
	index, err = newBlockFromHandle(indexHandle, r, true)
	if err != nil {
		return
	}

	dataEnd := metaHandle.Offset

	var filter *filterBlock
	filterPolicy := o.GetFilterPolicy()
	if filterPolicy != nil {
		var meta *block
		meta, err = newBlockFromHandle(metaHandle, r, true)
		if err != nil {
			return
		}
		iter := meta.NewIterator(o.GetComparator())
		key := "filter." + filterPolicy.Name()
		if iter.Seek([]byte(key)) && string(iter.Key()) == key {
			filterHandle := new(blockHandle)
			_, err = filterHandle.DecodeFrom(iter.Value())
			if err != nil {
				return
			}
			dataEnd = filterHandle.Offset
			filter, err = newFilterBlockFromHandle(filterHandle, r, true, filterPolicy)
			if err != nil {
				return
			}
		}
	}

	t = &Table{
		r:       r,
		o:       o,
		index:   index,
		filter:  filter,
		dataEnd: dataEnd,
		cacheId: cacheId,
	}
	return
}

func (t *Table) NewIterator(o *leveldb.ReadOptions) leveldb.Iterator {
	index_iter := t.index.NewIterator(t.o.GetComparator())
	return leveldb.NewTwoLevelIterator(index_iter, &blockGetter{t: t, o: o})
}

func (t *Table) Get(key []byte, o *leveldb.ReadOptions) (rkey, rvalue []byte, err error) {
	index_iter := t.index.NewIterator(t.o.GetComparator())
	if !index_iter.Seek(key) {
		err = index_iter.Error()
		if err == nil {
			err = leveldb.ErrNotFound
		}
		return
	}

	handle := new(blockHandle)
	_, err = handle.DecodeFrom(index_iter.Value())
	if err != nil {
		return
	}
	if t.filter == nil || t.filter.KeyMayMatch(uint(handle.Offset), key) {
		var iter leveldb.Iterator
		iter, err = t.getBlock(handle, o)
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

func (t *Table) ApproximateOffsetOf(key []byte) uint64 {
	index_iter := t.index.NewIterator(t.o.GetComparator())
	if index_iter.Seek(key) {
		handle := new(blockHandle)
		_, err := handle.DecodeFrom(index_iter.Value())
		if err == nil {
			return handle.Offset
		}
	}
	return t.dataEnd
}

func (t *Table) getBlock(handle *blockHandle, o *leveldb.ReadOptions) (iter leveldb.Iterator, err error) {
	var b *block
	newBlock := func() {
		b, err = newBlockFromHandle(handle, t.r, o.HasFlag(leveldb.RFVerifyChecksums))
	}

	var cacheObj leveldb.CacheObject
	cache := t.o.GetBlockCache()
	if cache != nil {
		buf := new(bytes.Buffer)
		buf.Grow(16)
		binary.Write(buf, binary.LittleEndian, t.cacheId)
		binary.Write(buf, binary.LittleEndian, handle.Offset)
		cacheKey := buf.Bytes()

		var ok bool
		cacheObj, ok = cache.Get(cacheKey)
		if ok {
			b = cacheObj.Value().(*block)
		} else {
			newBlock()
			if err != nil {
				return
			}
			cacheObj = cache.Set(cacheKey, b, int(handle.Size))
		}
	} else {
		newBlock()
		if err != nil {
			return
		}
	}

	iter = b.NewIterator(t.o.GetComparator())
	if cacheObj != nil {
		if biter, ok := iter.(*blockIterator); ok {
			setCacheFinalizer(biter, cacheObj)
		} else {
			cacheObj.Release()
		}
	}
	return
}

type blockGetter struct {
	t *Table
	o *leveldb.ReadOptions
}

func (g *blockGetter) Get(value []byte) (iter leveldb.Iterator, err error) {
	handle := new(blockHandle)
	_, err = handle.DecodeFrom(value)
	if err != nil {
		return
	}

	return g.t.getBlock(handle, g.o)
}

func setCacheFinalizer(x *blockIterator, cache leveldb.CacheObject) {
	runtime.SetFinalizer(x, func(x *blockIterator) {
		cache.Release()
	})
}
