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
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/iter"
)

var errIKeyCorrupt = errors.ErrCorrupt("internal key corrupted")

// Iterator represent an interator states over a database session.
type Iterator struct {
	cmp comparer.BasicComparer
	it  iter.Iterator
	seq uint64

	valid    bool
	backward bool
	last     bool
	skey     []byte
	sval     []byte
	err      error
}

func newIterator(seq uint64, it iter.Iterator, cmp comparer.BasicComparer) *Iterator {
	return &Iterator{cmp: cmp, it: it, seq: seq}
}

func (i *Iterator) clear() {
	i.skey, i.sval = nil, nil
}

func (i *Iterator) scanNext(skip []byte) {
	cmp := i.cmp
	it := i.it

	for {
		key := iKey(it.Key())
		if seq, t, ok := key.parseNum(); ok {
			if seq <= i.seq {
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
		} else {
			i.err = errIKeyCorrupt
			i.valid = false
			return
		}

		if !it.Next() {
			break
		}
	}

	i.valid = false
}

func (i *Iterator) scanPrev() {
	cmp := i.cmp
	it := i.it

	tt := tDel
	if it.Valid() {
		for {
			key := iKey(it.Key())
			if seq, t, ok := key.parseNum(); ok {
				if seq <= i.seq {
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
			} else {
				i.err = errIKeyCorrupt
				i.valid = false
				i.clear()
				return
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

func (i *Iterator) Valid() bool {
	return i.valid
}

func (i *Iterator) First() bool {
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

func (i *Iterator) Last() bool {
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

func (i *Iterator) Seek(key []byte) bool {
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

func (i *Iterator) Next() bool {
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

func (i *Iterator) Prev() bool {
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

func (i *Iterator) Key() []byte {
	if !i.valid {
		return nil
	}
	if i.backward {
		return i.skey
	}
	return iKey(i.it.Key()).ukey()
}

func (i *Iterator) Value() []byte {
	if !i.valid {
		return nil
	}
	if i.backward {
		return i.sval
	}
	return i.it.Value()
}

func (i *Iterator) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.it.Error()
}
