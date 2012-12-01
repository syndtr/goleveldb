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
)

type Table struct {
	r       leveldb.Reader
	o       *leveldb.Options

	meta    *block
	index   *block

	filter  *filterBlock

	dataEnd uint64
}

func NewTable(r leveldb.Reader, o *leveldb.Options) (t *Table, err error) {
	var metaHandle, indexHandle *blockHandle
	metaHandle, indexHandle, err = readFooter(r)
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
	}
	return
}

func (t *Table) NewIterator(o *leveldb.ReadOptions) leveldb.Iterator {
	index_iter := t.index.NewIterator(t.o.GetComparator())
	return leveldb.NewTwoLevelIterator(index_iter, &blockGetter{t: t, o: o})
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

	t := g.t

	var b *block
	b, err = newBlockFromHandle(handle, t.r, g.o.HasFlag(leveldb.RFVerifyChecksums))
	if err != nil {
		return
	}

	iter = b.NewIterator(t.o.GetComparator())
	return
}
