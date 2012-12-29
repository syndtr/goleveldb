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
	"bytes"
	"encoding/binary"
	"io"
	"leveldb"
	"leveldb/memdb"
)

var (
	errBatchTooShort  = leveldb.ErrCorrupt("batch in too short")
	errBatchBadRecord = leveldb.ErrCorrupt("bad record in batch")
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

type Batch struct {
	rec    []batchRecord
	seq    uint64
	kvSize int
	ch     []chan error
	sync   bool
}

func (b *Batch) Put(key, value []byte) {
	b.rec = append(b.rec, batchRecord{tVal, key, value})
	b.kvSize += len(key) + len(value)
}

func (b *Batch) Delete(key []byte) {
	b.rec = append(b.rec, batchRecord{tDel, key, nil})
	b.kvSize += len(key)
}

func (b *Batch) Reset() {
	b.rec = b.rec[:0]
	b.seq = 0
	b.kvSize = 0
	b.ch = nil
	b.sync = false
}

func (b *Batch) init(sync bool) chan error {
	ch := make(chan error)
	b.ch = nil
	b.ch = append(b.ch, ch)
	b.sync = sync
	return ch
}

func (b *Batch) done(err error) {
	for _, ch := range b.ch {
		ch <- err
	}
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
	b.ch = append(b.ch, p.ch...)
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

func (b *Batch) encodeTo(w io.Writer) (err error) {
	// write seq number
	err = binary.Write(w, binary.LittleEndian, b.seq)
	if err != nil {
		return
	}

	// write record len
	err = binary.Write(w, binary.LittleEndian, uint32(len(b.rec)))
	if err != nil {
		return
	}

	tmp := make([]byte, binary.MaxVarintLen32)

	putUvarint := func(p uint64) (err error) {
		n := binary.PutUvarint(tmp, p)
		_, err = w.Write(tmp[:n])
		return
	}

	putBytes := func(p []byte) (err error) {
		err = putUvarint(uint64(len(p)))
		if err != nil {
			return
		}
		_, err = w.Write(p)
		if err != nil {
			return
		}
		return
	}

	for _, rec := range b.rec {
		// write record type
		_, err = w.Write([]byte{byte(rec.t)})
		if err != nil {
			return
		}

		// write key
		err = putBytes(rec.key)
		if err != nil {
			return
		}

		// write value if record type is 'value'
		if rec.t == tVal {
			err = putBytes(rec.value)
			if err != nil {
				return
			}
		}
	}

	return
}

func (b *Batch) encode() []byte {
	buf := new(bytes.Buffer)
	b.encodeTo(buf)
	return buf.Bytes()
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
			err = leveldb.ErrCorrupt("invalid batch record type in batch")
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
		err = leveldb.ErrCorrupt("invalid batch record length")
	}

	return
}
