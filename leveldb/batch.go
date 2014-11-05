// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"errors"

	"github.com/syndtr/goleveldb/leveldb/memdb"
)

var (
	errBatchTooShort  = errors.New("leveldb: batch is too short")
	errBatchBadRecord = errors.New("leveldb: bad record in batch")
)

const kBatchHdrLen = 8 + 4

type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
}

// Batch is a write batch.
type Batch struct {
	data       []byte
	rLen, bLen int
	seq        uint64
	sync       bool
}

func (b *Batch) grow(n int) {
	off := len(b.data)
	if off == 0 {
		// include headers
		off = kBatchHdrLen
		n += off
	}
	if cap(b.data)-off >= n {
		return
	}
	data := make([]byte, 2*cap(b.data)+n)
	copy(data, b.data)
	b.data = data[:off]
}

func (b *Batch) appendRec(t vType, key, value []byte) {
	n := 1 + binary.MaxVarintLen32 + len(key)
	if t == tVal {
		n += binary.MaxVarintLen32 + len(value)
	}
	b.grow(n)
	off := len(b.data)
	data := b.data[:off+n]
	data[off] = byte(t)
	off += 1
	off += binary.PutUvarint(data[off:], uint64(len(key)))
	copy(data[off:], key)
	off += len(key)
	if t == tVal {
		off += binary.PutUvarint(data[off:], uint64(len(value)))
		copy(data[off:], value)
		off += len(value)
	}
	b.data = data[:off]
	b.rLen++
	//  Include 8-byte ikey header
	b.bLen += len(key) + len(value) + 8
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns.
func (b *Batch) Put(key, value []byte) {
	b.appendRec(tVal, key, value)
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns.
func (b *Batch) Delete(key []byte) {
	b.appendRec(tDel, key, nil)
}

// Dump dumps batch contents. The returned slice can be loaded into the
// batch using Load method.
// The returned slice is not its own copy, so the contents should not be
// modified.
func (b *Batch) Dump() []byte {
	return b.encode()
}

// Load loads given slice into the batch. Previous contents of the batch
// will be discarded.
// The given slice will not be copied and will be used as batch buffer, so
// it is not safe to modify the contents of the slice.
func (b *Batch) Load(data []byte) error {
	return b.decode(data)
}

// Replay replays batch contents.
func (b *Batch) Replay(r BatchReplay) error {
	return b.decodeRec(func(i int, t vType, key, value []byte) {
		switch t {
		case tVal:
			r.Put(key, value)
		case tDel:
			r.Delete(key)
		}
	})
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.data = nil
	b.seq = 0
	b.rLen = 0
	b.bLen = 0
	b.sync = false
}

func (b *Batch) init(sync bool) {
	b.sync = sync
}

func (b *Batch) append(p *Batch) {
	if p.rLen > 0 {
		b.grow(len(p.data) - kBatchHdrLen)
		b.data = append(b.data, p.data[kBatchHdrLen:]...)
		b.rLen += p.rLen
	}
	if p.sync {
		b.sync = true
	}
}

func (b *Batch) len() int {
	return b.rLen
}

func (b *Batch) size() int {
	return b.bLen
}

func (b *Batch) encode() []byte {
	b.grow(0)
	binary.LittleEndian.PutUint64(b.data, b.seq)
	binary.LittleEndian.PutUint32(b.data[8:], uint32(b.rLen))

	return b.data
}

func (b *Batch) decode(data []byte) error {
	if len(data) < kBatchHdrLen {
		return errBatchTooShort
	}

	b.seq = binary.LittleEndian.Uint64(data)
	b.rLen = int(binary.LittleEndian.Uint32(data[8:]))
	// No need to be precise at this point, it won't be used anyway
	b.bLen = len(data) - kBatchHdrLen
	b.data = data

	return nil
}

func (b *Batch) decodeRec(f func(i int, t vType, key, value []byte)) error {
	off := kBatchHdrLen
	for i := 0; i < b.rLen; i++ {
		if off >= len(b.data) {
			return errors.New("leveldb: invalid batch record length")
		}

		t := vType(b.data[off])
		if t > tVal {
			return errors.New("leveldb: invalid batch record type in batch")
		}
		off += 1

		x, n := binary.Uvarint(b.data[off:])
		off += n
		if n <= 0 || off+int(x) > len(b.data) {
			return errBatchBadRecord
		}
		key := b.data[off : off+int(x)]
		off += int(x)

		var value []byte
		if t == tVal {
			x, n := binary.Uvarint(b.data[off:])
			off += n
			if n <= 0 || off+int(x) > len(b.data) {
				return errBatchBadRecord
			}
			value = b.data[off : off+int(x)]
			off += int(x)
		}

		f(i, t, key, value)
	}

	return nil
}

func (b *Batch) memReplay(to *memdb.DB) error {
	return b.decodeRec(func(i int, t vType, key, value []byte) {
		ikey := newIKey(key, b.seq+uint64(i), t)
		to.Put(ikey, value)
	})
}

func (b *Batch) revertMemReplay(to *memdb.DB) error {
	return b.decodeRec(func(i int, t vType, key, value []byte) {
		ikey := newIKey(key, b.seq+uint64(i), t)
		to.Delete(ikey)
	})
}
