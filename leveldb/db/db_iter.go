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

import "leveldb"

var errIKeyCorrupt = leveldb.ErrCorrupt("internal key corrupted")

type DBIter struct {
	cmp  leveldb.BasicComparer
	iter leveldb.Iterator
	seq  uint64

	valid    bool
	backward bool
	last     bool
	skey     []byte
	sval     []byte
	err      error
}

func newDBIter(seq uint64, iter leveldb.Iterator, cmp leveldb.BasicComparer) *DBIter {
	return &DBIter{cmp: cmp, iter: iter, seq: seq}
}

func (i *DBIter) clear() {
	i.skey, i.sval = nil, nil
}

func (i *DBIter) scanNext(skip []byte) {
	cmp := i.cmp
	iter := i.iter

	for {
		pkey := iKey(iter.Key()).parse()
		if pkey == nil {
			i.err = errIKeyCorrupt
			i.valid = false
			return
		}
		if pkey.sequence <= i.seq {
			switch pkey.vtype {
			case tDel:
				skip = pkey.ukey
			case tVal:
				if skip == nil || cmp.Compare(pkey.ukey, skip) > 0 {
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

func (i *DBIter) scanPrev() {
	cmp := i.cmp
	iter := i.iter

	vtype := tDel
	if iter.Valid() {
		for {
			pkey := iKey(iter.Key()).parse()
			if pkey == nil {
				i.err = errIKeyCorrupt
				i.valid = false
				i.clear()
				return
			}
			if pkey.sequence <= i.seq {
				if vtype != tDel && cmp.Compare(pkey.ukey, i.skey) < 0 {
					break
				}
				vtype = pkey.vtype
				if vtype == tDel {
					i.skey = nil
				} else {
					i.skey = pkey.ukey
					i.sval = iter.Value()
				}
			}
			if !iter.Prev() {
				break
			}
		}
	}

	if vtype == tDel {
		i.valid = false
		i.clear()
		i.backward = false
	} else {
		i.valid = true
	}
}

func (i *DBIter) Valid() bool {
	return i.valid
}

func (i *DBIter) First() bool {
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

func (i *DBIter) Last() bool {
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

func (i *DBIter) Seek(key []byte) bool {
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

func (i *DBIter) Next() bool {
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

func (i *DBIter) Prev() bool {
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

func (i *DBIter) Key() []byte {
	if !i.valid {
		return nil
	}
	if i.backward {
		return i.skey
	}
	return iKey(i.iter.Key()).ukey()
}

func (i *DBIter) Value() []byte {
	if !i.valid {
		return nil
	}
	if i.backward {
		return i.sval
	}
	return i.iter.Value()
}

func (i *DBIter) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.iter.Error()
}
