// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"

	"snappy"

	"leveldb/block"
	"leveldb/comparer"
	"leveldb/descriptor"
	"leveldb/hash"
	"leveldb/opt"
)

// Writer represent a table writer.
type Writer struct {
	w   descriptor.Writer
	o   opt.OptionsGetter
	cmp comparer.Comparer

	data   *block.Writer
	index  *block.Writer
	filter *block.FilterWriter

	n, off int
	lkey   []byte // last key
	lblock *bInfo // last block
	pindex bool   // pending index

	closed bool
}

// NewWriter create new initialized table writer.
func NewWriter(w descriptor.Writer, o opt.OptionsGetter) *Writer {
	t := &Writer{w: w, o: o, cmp: o.GetComparer()}
	// Creating blocks
	t.data = block.NewWriter(o.GetBlockRestartInterval())
	t.index = block.NewWriter(1)
	filter := o.GetFilter()
	if filter != nil {
		t.filter = block.NewFilterWriter(filter)
		t.filter.Generate(0)
	}
	t.lblock = new(bInfo)
	return t
}

// Add append key/value to the table.
func (t *Writer) Add(key, value []byte) (err error) {
	if t.closed {
		panic("operation on closed table writer")
	}

	if t.pindex {
		// write the pending index
		sep := t.cmp.Separator(t.lkey, key)
		t.index.Add(sep, t.lblock.encode())
		t.pindex = false
	}

	if t.filter != nil {
		t.filter.Add(key)
	}

	t.lkey = key
	t.n++

	t.data.Add(key, value)
	if t.data.Size() >= t.o.GetBlockSize() {
		err = t.Flush()
	}
	return
}

// Flush finalize and write the data block.
func (t *Writer) Flush() (err error) {
	if t.closed {
		panic("operation on closed table writer")
	}

	if t.pindex {
		return
	}

	err = t.write(t.data.Finish(), t.lblock, false)
	if err != nil {
		return
	}
	t.data.Reset()

	t.pindex = true

	if t.filter != nil {
		t.filter.Generate(t.off)
	}
	return
}

// Finish finalize the table. No Add(), Flush() or Finish() is possible
// beyond this, doing so will raise panic.
func (t *Writer) Finish() (err error) {
	if t.closed {
		panic("operation on closed table writer")
	}

	err = t.Flush()
	if err != nil {
		return
	}

	t.closed = true

	// Write filter block
	fi := new(bInfo)
	if t.filter != nil {
		err = t.write(t.filter.Finish(), fi, true)
		if err != nil {
			return
		}
	}

	// Write meta block
	meta := block.NewWriter(t.o.GetBlockRestartInterval())
	if t.filter != nil {
		filter := t.o.GetFilter()
		key := []byte("filter." + filter.Name())
		meta.Add(key, fi.encode())
	}
	mb := new(bInfo)
	err = t.write(meta.Finish(), mb, false)
	if err != nil {
		return
	}

	// Write index block
	if t.pindex {
		suc := t.cmp.Successor(t.lkey)
		t.index.Add(suc, t.lblock.encode())
		t.pindex = false
	}
	ib := new(bInfo)
	err = t.write(t.index.Finish(), ib, false)
	if err != nil {
		return
	}

	// Write footer
	var n int
	n, err = writeFooter(t.w, mb, ib)
	if err != nil {
		return
	}
	t.off += n

	return
}

// Len return the number of records added so far.
func (t *Writer) Len() int {
	return t.n
}

// Size return the number of bytes written so far.
func (t *Writer) Size() int {
	return t.off
}

// CountBlock return the number of data block written so far.
func (t *Writer) CountBlock() int {
	n := t.index.Len()
	if !t.closed {
		n++
	}
	return n
}

func (t *Writer) write(buf []byte, bi *bInfo, raw bool) (err error) {
	compression := opt.NoCompression
	if !raw {
		compression = t.o.GetCompressionType()
	}
	switch compression {
	case opt.SnappyCompression:
		buf, err = snappy.Encode(nil, buf)
		if err != nil {
			return
		}
	}

	if bi != nil {
		bi.offset = uint64(t.off)
		bi.size = uint64(len(buf))
	}

	_, err = t.w.Write(buf)
	if err != nil {
		return
	}

	compbit := []byte{byte(compression)}
	_, err = t.w.Write(compbit)
	if err != nil {
		return
	}

	crc := hash.NewCRC32C()
	crc.Write(buf)
	crc.Write(compbit)
	err = binary.Write(t.w, binary.LittleEndian, hash.MaskCRC32(crc.Sum32()))
	if err != nil {
		return
	}

	t.off += len(buf) + 5
	return
}
