// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func (d *DB) newRawIterator(ro *opt.ReadOptions) iterator.Iterator {
	s := d.s

	mem := d.getMem()
	v := s.version()
	defer v.release()

	tableIters := v.getIterators(ro)
	iters := make([]iterator.Iterator, 0, len(tableIters)+2)
	iters = append(iters, mem.cur.NewIterator())
	if mem.froze != nil {
		iters = append(iters, mem.froze.NewIterator())
	}
	iters = append(iters, tableIters...)
	return iterator.NewMergedIterator(iters, s.cmp)
}

func (d *DB) newIterator(seq uint64, ro *opt.ReadOptions) *dbIter {
	rawIter := d.newRawIterator(ro)
	iter := &dbIter{
		cmp:        d.s.cmp.cmp,
		iter:       rawIter,
		seq:        seq,
		copyBuffer: !ro.HasFlag(opt.RFDontCopyBuffer),
	}
	return iter
}

// dbIter represent an interator states over a database session.
type dbIter struct {
	cmp        comparer.BasicComparer
	iter       iterator.Iterator
	seq        uint64
	copyBuffer bool

	released bool
	valid    bool
	backward bool
	last     bool
	skey     []byte
	sval     []byte
}

func (i *dbIter) clear() {
	i.skey, i.sval = nil, nil
}

func (i *dbIter) scanNext(skip []byte) {
	cmp := i.cmp
	iter := i.iter

	for {
		key := iKey(iter.Key())
		if seq, t, ok := key.parseNum(); ok && seq <= i.seq {
			switch t {
			case tDel:
				skip = key.ukey()
			case tVal:
				if skip == nil || cmp.Compare(key.ukey(), skip) > 0 {
					i.valid = true
					return
				}
			}
		}

		if !iter.Next() {
			break
		}
	}

	i.valid = false
}

func (i *dbIter) scanPrev() {
	cmp := i.cmp
	iter := i.iter

	tt := tDel
	if iter.Valid() {
		for {
			key := iKey(iter.Key())
			if seq, t, ok := key.parseNum(); ok && seq <= i.seq {
				if tt != tDel && cmp.Compare(key.ukey(), i.skey) < 0 {
					break
				}

				if t == tDel {
					i.skey = nil
				} else {
					i.skey = key.ukey()
					i.sval = iter.Value()
				}
				tt = t
			}

			if !iter.Prev() {
				break
			}
		}
	}

	if tt == tDel {
		i.valid = false
		i.clear()
		i.backward = false
	} else {
		i.valid = true
	}
}

func (i *dbIter) Valid() bool {
	return i.valid
}

func (i *dbIter) First() bool {
	if i.released {
		return false
	}

	i.clear()
	i.last = false
	i.backward = false
	if i.iter.First() {
		i.scanNext(nil)
	} else {
		i.valid = false
	}
	i.last = false
	return i.valid
}

func (i *dbIter) Last() bool {
	if i.released {
		return false
	}

	i.clear()
	i.last = false
	i.backward = true
	if i.iter.Last() {
		i.scanPrev()
	} else {
		i.valid = false
	}
	i.last = false
	return i.valid
}

func (i *dbIter) Seek(key []byte) bool {
	if i.released {
		return false
	}

	i.clear()
	i.last = false
	i.backward = false
	ikey := newIKey(key, i.seq, tSeek)
	if i.iter.Seek(ikey) {
		i.scanNext(nil)
	} else {
		i.valid = false
	}
	i.last = !i.valid
	return i.valid
}

func (i *dbIter) Next() bool {
	if i.released {
		return false
	}

	iter := i.iter

	if !i.valid {
		if !i.last {
			return i.First()
		}
		return false
	}

	if i.backward {
		i.clear()
		i.backward = false
		if !iter.Next() {
			i.valid = false
			return false
		}
	}

	ikey := iKey(iter.Key())
	i.scanNext(ikey.ukey())
	i.last = !i.valid
	return i.valid
}

func (i *dbIter) Prev() bool {
	if i.released {
		return false
	}

	cmp := i.cmp
	iter := i.iter

	if !i.valid {
		if i.last {
			return i.Last()
		}
		return false
	}

	if !i.backward {
		lkey := iKey(iter.Key()).ukey()
		for {
			if !iter.Prev() {
				i.valid = false
				return false
			}
			ukey := iKey(iter.Key()).ukey()
			if cmp.Compare(ukey, lkey) < 0 {
				break
			}
		}
		i.backward = true
	}

	i.scanPrev()
	return i.valid
}

func (i *dbIter) Key() []byte {
	if !i.valid {
		return nil
	}
	var ret []byte
	if i.backward {
		ret = i.skey
	} else {
		ret = iKey(i.iter.Key()).ukey()
	}
	if i.copyBuffer {
		return dupBytes(ret)
	}
	return ret
}

func (i *dbIter) Value() []byte {
	if !i.valid {
		return nil
	}
	var ret []byte
	if i.backward {
		ret = i.sval
	} else {
		ret = i.iter.Value()
	}
	if i.copyBuffer {
		return dupBytes(ret)
	}
	return ret
}

func (i *dbIter) Release() {
	if !i.released {
		i.released = true
		i.valid = false
		i.skey = nil
		i.sval = nil
		i.iter.Release()
	}
}

func (i *dbIter) Error() error {
	if i.released {
		return errors.New("interator released")
	}
	return i.iter.Error()
}
