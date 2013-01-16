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
	"encoding/binary"
	"leveldb/errors"
	"leveldb/memdb"
)

var (
	errBatchTooShort  = errors.ErrCorrupt("batch in too short")
	errBatchBadRecord = errors.ErrCorrupt("bad record in batch")
)

type batchReplay interface {
	put(key, value []byte, seq uint64)
	delete(key []byte, seq uint64)
}

type memBatch struct {
	mem *memdb.DB
}

func (p *memBatch) put(key, value []byte, seq uint64) {
	ikey := newIKey(key, seq, tVal)
	p.mem.Put(ikey, value)
}

func (p *memBatch) delete(key []byte, seq uint64) {
	ikey := newIKey(key, seq, tDel)
	p.mem.Put(ikey, nil)
}

type batchRecord struct {
	t          vType
	key, value []byte
}

// Batch represent a write batch.
type Batch struct {
	rec    []batchRecord
	seq    uint64
	kvSize int
	sync   bool
}

// Put put given key/value to the batch for insert operation.
func (b *Batch) Put(key, value []byte) {
	b.rec = append(b.rec, batchRecord{tVal, key, value})
	b.kvSize += len(key) + len(value)
}

// Delete put given key to the batch for delete operation.
func (b *Batch) Delete(key []byte) {
	b.rec = append(b.rec, batchRecord{tDel, key, nil})
	b.kvSize += len(key)
}

// Reset reset contents of the batch.
func (b *Batch) Reset() {
	b.rec = b.rec[:0]
	b.seq = 0
	b.kvSize = 0
	b.sync = false
}

func (b *Batch) init(sync bool) {
	b.sync = sync
}

func (b *Batch) put(key, value []byte, seq uint64) {
	if len(b.rec) == 0 {
		b.seq = seq
	}
	b.Put(key, value)
}

func (b *Batch) delete(key []byte, seq uint64) {
	if len(b.rec) == 0 {
		b.seq = seq
	}
	b.Delete(key)
}

func (b *Batch) append(p *Batch) {
	b.rec = append(b.rec, p.rec...)
	b.kvSize += p.kvSize
	if p.sync {
		b.sync = true
	}
}

func (b *Batch) len() int {
	return len(b.rec)
}

func (b *Batch) size() int {
	return b.kvSize
}

func (b *Batch) encode() []byte {
	x := 8 + 4 + binary.MaxVarintLen32*2*len(b.rec) + b.kvSize
	buf := make([]byte, x)

	i := 8 + 4
	binary.LittleEndian.PutUint64(buf, b.seq)
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(b.rec)))

	putBytes := func(p []byte) {
		i += binary.PutUvarint(buf[i:], uint64(len(p)))
		copy(buf[i:], p)
		i += len(p)
	}

	for _, rec := range b.rec {
		// write record type
		buf[i] = byte(rec.t)
		i += 1

		// write key
		putBytes(rec.key)

		// write value if record type is 'value'
		if rec.t == tVal {
			putBytes(rec.value)
		}
	}

	return buf[:i]
}

func (b *Batch) replay(to batchReplay) {
	for i, rec := range b.rec {
		switch rec.t {
		case tVal:
			to.put(rec.key, rec.value, b.seq+uint64(i))
		case tDel:
			to.delete(rec.key, b.seq+uint64(i))
		}
	}
}

func (b *Batch) memReplay(to *memdb.DB) {
	for i, rec := range b.rec {
		ikey := newIKey(rec.key, b.seq+uint64(i), rec.t)
		to.Put(ikey, rec.value)
	}
}

func (b *Batch) revertMemReplay(to *memdb.DB) {
	for i, rec := range b.rec {
		ikey := newIKey(rec.key, b.seq+uint64(i), rec.t)
		to.Remove(ikey)
	}
}

func decodeBatchHeader(b []byte, seq *uint64, n *uint32) (err error) {
	if len(b) < 12 {
		return errBatchTooShort
	}

	*seq = binary.LittleEndian.Uint64(b)
	*n = binary.LittleEndian.Uint32(b[8:])
	return
}

func replayBatch(b []byte, to batchReplay) (seq uint64, err error) {
	if len(b) < 12 {
		return 0, errBatchTooShort
	}

	var rn uint32
	err = decodeBatchHeader(b, &seq, &rn)
	if err != nil {
		return
	}

	fseq := seq
	b = b[12:]

	for ; len(b) > 0; seq++ {
		t := vType(b[0])
		if t > tVal {
			err = errors.ErrCorrupt("invalid batch record type in batch")
			return
		}

		b = b[1:]

		var valid bool
		var key, value []byte
		valid, key, b = sliceBytesTest(b)
		if !valid {
			err = errBatchBadRecord
			return
		}

		switch t {
		case tVal:
			valid, value, b = sliceBytesTest(b)
			if !valid {
				err = errBatchBadRecord
				return
			}
			to.put(key, value, seq)
		case tDel:
			to.delete(key, seq)
		}
	}

	if seq-fseq != uint64(rn) {
		err = errors.ErrCorrupt("invalid batch record length")
	}

	return
}
