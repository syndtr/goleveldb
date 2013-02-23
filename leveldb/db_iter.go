// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/iterator"
	"leveldb/opt"
)

var errIKeyCorrupt = errors.ErrCorrupt("internal key corrupted")

// newRawIterator return merged interators of current version, current frozen memdb
// and current memdb.
func (d *DB) newRawIterator(ro *opt.ReadOptions) iterator.Iterator {
	s := d.s

	mem := d.getMem()
	v := s.version()

	ti := v.getIterators(ro)
	ii := make([]iterator.Iterator, 0, len(ti)+2)
	ii = append(ii, mem.cur.NewIterator())
	if mem.froze != nil {
		ii = append(ii, mem.froze.NewIterator())
	}
	ii = append(ii, ti...)

	return iterator.NewMergedIterator(ii, s.cmp)
}

// dbIter represent an interator states over a database session.
type dbIter struct {
	snap *Snapshot
	cmp  comparer.BasicComparer
	it   iterator.Iterator
	seq  uint64

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
	it := i.it

	for {
		key := iKey(it.Key())
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

		if !it.Next() {
			break
		}
	}

	i.valid = false
}

func (i *dbIter) scanPrev() {
	cmp := i.cmp
	it := i.it

	tt := tDel
	if it.Valid() {
		for {
			key := iKey(it.Key())
			if seq, t, ok := key.parseNum(); ok && seq <= i.seq {
				if tt != tDel && cmp.Compare(key.ukey(), i.skey) < 0 {
					break
				}

				if t == tDel {
					i.skey = nil
				} else {
					i.skey = key.ukey()
					i.sval = it.Value()
				}
				tt = t
			}

			if !it.Prev() {
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

func (i *dbIter) isOk() bool {
	return i.snap.isOk()
}

func (i *dbIter) Valid() bool {
	return i.valid && i.isOk()
}

func (i *dbIter) First() bool {
	if !i.isOk() {
		return false
	}

	i.clear()
	i.last = false
	i.backward = false
	if i.it.First() {
		i.scanNext(nil)
	} else {
		i.valid = false
	}
	i.last = false
	return i.valid
}

func (i *dbIter) Last() bool {
	if !i.isOk() {
		return false
	}

	i.clear()
	i.last = false
	i.backward = true
	if i.it.Last() {
		i.scanPrev()
	} else {
		i.valid = false
	}
	i.last = false
	return i.valid
}

func (i *dbIter) Seek(key []byte) bool {
	if !i.isOk() {
		return false
	}

	i.clear()
	i.last = false
	i.backward = false
	ikey := newIKey(key, i.seq, tSeek)
	if i.it.Seek(ikey) {
		i.scanNext(nil)
	} else {
		i.valid = false
	}
	i.last = !i.valid
	return i.valid
}

func (i *dbIter) Next() bool {
	if !i.isOk() {
		return false
	}

	it := i.it

	if !i.valid {
		if !i.last {
			return i.First()
		}
		return false
	}

	if i.backward {
		i.clear()
		i.backward = false
		if !it.Next() {
			i.valid = false
			return false
		}
	}

	ikey := iKey(it.Key())
	i.scanNext(ikey.ukey())
	i.last = !i.valid
	return i.valid
}

func (i *dbIter) Prev() bool {
	if !i.isOk() {
		return false
	}

	cmp := i.cmp
	it := i.it

	if !i.valid {
		if i.last {
			return i.Last()
		}
		return false
	}

	if !i.backward {
		lkey := iKey(it.Key()).ukey()
		for {
			if !it.Prev() {
				i.valid = false
				return false
			}
			ukey := iKey(it.Key()).ukey()
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
	if !i.valid || !i.isOk() {
		return nil
	}
	if i.backward {
		return i.skey
	}
	return iKey(i.it.Key()).ukey()
}

func (i *dbIter) Value() []byte {
	if !i.valid || !i.isOk() {
		return nil
	}
	if i.backward {
		return i.sval
	}
	return i.it.Value()
}

func (i *dbIter) Error() error {
	if err := i.snap.ok(); err != nil {
		return err
	}
	return i.it.Error()
}
