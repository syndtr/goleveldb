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

// Package table allows read and write sorted key/value.
package table

import (
	"leveldb/block"
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/descriptor"
	"leveldb/errors"
	"leveldb/iter"
	"leveldb/opt"
	"runtime"
)

// Reader represent a table reader.
type Reader struct {
	r descriptor.Reader
	o opt.OptionsGetter

	meta   *block.Reader
	index  *block.Reader
	filter *block.FilterReader

	dataEnd uint64
	cache   cache.Namespace
}

// NewReader create new initialized table reader.
func NewReader(r descriptor.Reader, size uint64, o opt.OptionsGetter, cache cache.Namespace) (p *Reader, err error) {
	mb, ib, err := readFooter(r, size)
	if err != nil {
		return
	}

	t := &Reader{r: r, o: o, dataEnd: mb.offset, cache: cache}

	// index block
	buf, err := ib.readAll(r, true)
	if err != nil {
		return
	}
	t.index, err = block.NewReader(buf, o.GetComparer())
	if err != nil {
		return
	}

	// filter block
	filter := o.GetFilter()
	if filter != nil {
		// we will ignore any errors at meta/filter block
		// since it is not essential for operation

		// meta block
		buf, err = mb.readAll(r, true)
		if err != nil {
			goto out
		}
		var meta *block.Reader
		meta, err = block.NewReader(buf, comparer.BytesComparer{})
		if err != nil {

			goto out
		}

		// check for filter name
		iter := meta.NewIterator()
		key := "filter." + filter.Name()
		if iter.Seek([]byte(key)) && string(iter.Key()) == key {
			fb := new(bInfo)
			_, err = fb.decodeFrom(iter.Value())
			if err != nil {
				return
			}

			// now the data end before filter block start offset
			t.dataEnd = fb.offset

			// filter block
			buf, err = fb.readAll(r, true)
			if err != nil {
				goto out
			}
			t.filter, err = block.NewFilterReader(buf, filter)
			if err != nil {
				goto out
			}
		}
	}

out:
	return t, nil
}

// NewIterator create new iterator over the table.
func (t *Reader) NewIterator(ro opt.ReadOptionsGetter) iter.Iterator {
	index_iter := &indexIter{t: t, ro: ro}
	t.index.InitIterator(&index_iter.Iterator)
	return iter.NewIndexedIterator(index_iter)
}

// Get lookup for given key on the table. Get returns errors.ErrNotFound if
// given key did not exist.
func (t *Reader) Get(key []byte, ro opt.ReadOptionsGetter) (rkey, rvalue []byte, err error) {
	// create an iterator of index block
	index_iter := t.index.NewIterator()
	if !index_iter.Seek(key) {
		err = index_iter.Error()
		if err == nil {
			err = errors.ErrNotFound
		}
		return
	}

	// decode data block info
	bi := new(bInfo)
	_, err = bi.decodeFrom(index_iter.Value())
	if err != nil {
		return
	}

	// get the data block
	if t.filter == nil || t.filter.KeyMayMatch(uint(bi.offset), key) {
		var it iter.Iterator
		var cache cache.Object
		it, cache, err = t.getDataIter(bi, ro)
		if err != nil {
			return
		}
		if cache != nil {
			defer cache.Release()
		}

		// seek to key
		if !it.Seek(key) {
			err = it.Error()
			if err == nil {
				err = errors.ErrNotFound
			}
			return
		}
		rkey, rvalue = it.Key(), it.Value()
	} else {
		err = errors.ErrNotFound
	}
	return
}

// ApproximateOffsetOf approximate the offset of given key in bytes.
func (t *Reader) ApproximateOffsetOf(key []byte) uint64 {
	index_iter := t.index.NewIterator()
	if index_iter.Seek(key) {
		bi := new(bInfo)
		_, err := bi.decodeFrom(index_iter.Value())
		if err == nil {
			return bi.offset
		}
	}
	// block info is corrupted or key is past the last key in the file.
	// Approximate the offset by returning offset of the end of data
	// block (which is right near the end of the file).
	return t.dataEnd
}

func (t *Reader) getDataIter(bi *bInfo, ro opt.ReadOptionsGetter) (it *block.Iterator, cache cache.Object, err error) {
	var b *block.Reader

	if t.cache != nil {
		cache, _ = t.cache.Get(bi.offset, func() (ok bool, value interface{}, charge int, fin func()) {
			var buf []byte
			buf, err = bi.readAll(t.r, ro.HasFlag(opt.RFVerifyChecksums))
			if err != nil {
				return
			}
			b, err = block.NewReader(buf, t.o.GetComparer())
			if err != nil {
				return
			}
			ok = true
			value = b
			charge = int(bi.size)
			return
		})

		if err != nil {
			return
		}

		if b == nil {
			b = cache.Value().(*block.Reader)
		}
	} else {
		var buf []byte
		buf, err = bi.readAll(t.r, ro.HasFlag(opt.RFVerifyChecksums))
		if err != nil {
			return
		}
		b, err = block.NewReader(buf, t.o.GetComparer())
		if err != nil {
			return
		}
	}

	it = b.NewIterator()
	return
}

type indexIter struct {
	block.Iterator

	t  *Reader
	ro opt.ReadOptionsGetter
}

func (i *indexIter) Get() (it iter.Iterator, err error) {
	bi := new(bInfo)
	_, err = bi.decodeFrom(i.Value())
	if err != nil {
		return
	}

	x, cache, err := i.t.getDataIter(bi, i.ro)
	if err != nil {
		return
	}
	if cache != nil {
		runtime.SetFinalizer(x, func(x *block.Iterator) {
			cache.Release()
		})
	}
	return x, nil
}
