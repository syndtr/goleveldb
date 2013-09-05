// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"

	"code.google.com/p/snappy-go/snappy"

	"github.com/syndtr/goleveldb/leveldb/block"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/hash"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

const (
	// Written to disk; don't modify.
	kNoCompression     = 0
	kSnappyCompression = 1
)

// Writer represent a table writer.
type Writer struct {
	w      storage.Writer
	o      opt.OptionsGetter
	cmp    comparer.Comparer
	filter filter.Filter

	dataBlock   *block.Writer
	indexBlock  *block.Writer
	filterBlock *block.FilterWriter

	n, off int
	lkey   []byte // last key
	lblock *bInfo // last block
	pindex bool   // pending index

	closed bool
}

// NewWriter create new initialized table writer.
func NewWriter(w storage.Writer, o opt.OptionsGetter) *Writer {
	t := &Writer{w: w, o: o, cmp: o.GetComparer()}
	// Creating blocks
	t.dataBlock = block.NewWriter(o.GetBlockRestartInterval())
	t.indexBlock = block.NewWriter(1)
	t.filter = o.GetFilter()
	if t.filter != nil {
		t.filterBlock = block.NewFilterWriter(t.filter)
		t.filterBlock.Generate(0)
	}
	t.lblock = new(bInfo)
	return t
}

// Add append key/value to the table.
func (t *Writer) Add(key, value []byte) error {
	if t.closed {
		panic("operation on closed table writer")
	}

	if t.pindex {
		// write the pending index
		sep := t.cmp.Separator(t.lkey, key)
		t.indexBlock.Add(sep, t.lblock.encode())
		t.pindex = false
	}

	if t.filterBlock != nil {
		t.filterBlock.Add(key)
	}

	t.lkey = key
	t.n++

	t.dataBlock.Add(key, value)
	if t.dataBlock.Size() >= t.o.GetBlockSize() {
		return t.Flush()
	}
	return nil
}

// Flush finalize and write the data block.
func (t *Writer) Flush() error {
	if t.closed {
		panic("operation on closed table writer")
	}

	if t.pindex {
		return nil
	}

	if err := t.write(t.dataBlock.Finish(), t.lblock, false); err != nil {
		return err
	}
	t.dataBlock.Reset()

	t.pindex = true

	if t.filterBlock != nil {
		t.filterBlock.Generate(t.off)
	}
	return nil
}

// Finish finalize the table. No Add(), Flush() or Finish() is possible
// beyond this, doing so will raise panic.
func (t *Writer) Finish() error {
	if t.closed {
		panic("operation on closed table writer")
	}

	if err := t.Flush(); err != nil {
		return err
	}

	t.closed = true

	// Write filter block
	fi := new(bInfo)
	if t.filterBlock != nil {
		if err := t.write(t.filterBlock.Finish(), fi, true); err != nil {
			return err
		}
	}

	// Write meta block
	meta := block.NewWriter(t.o.GetBlockRestartInterval())
	if t.filter != nil {
		key := []byte("filter." + t.filter.Name())
		meta.Add(key, fi.encode())
	}
	mb := new(bInfo)
	if err := t.write(meta.Finish(), mb, false); err != nil {
		return err
	}

	// Write index block
	if t.pindex {
		suc := t.cmp.Successor(t.lkey)
		t.indexBlock.Add(suc, t.lblock.encode())
		t.pindex = false
	}
	ib := new(bInfo)
	if err := t.write(t.indexBlock.Finish(), ib, false); err != nil {
		return err
	}

	// Write footer
	n, err := writeFooter(t.w, mb, ib)
	if err != nil {
		return err
	}
	t.off += n

	return nil
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
	n := t.indexBlock.Len()
	if !t.closed {
		n++
	}
	return n
}

func (t *Writer) write(buf []byte, bi *bInfo, raw bool) (err error) {
	compression := kNoCompression
	if !raw {
		switch t.o.GetCompressionType() {
		case opt.DefaultCompression, opt.SnappyCompression:
			compression = kSnappyCompression
			buf, err = snappy.Encode(nil, buf)
			if err != nil {
				return err
			}
		}
	}

	if bi != nil {
		bi.offset = uint64(t.off)
		bi.size = uint64(len(buf))
	}

	if _, err = t.w.Write(buf); err != nil {
		return err
	}

	compbit := []byte{byte(compression)}
	if _, err = t.w.Write(compbit); err != nil {
		return err
	}

	crc := hash.NewCRC32C()
	crc.Write(buf)
	crc.Write(compbit)
	if err = binary.Write(t.w, binary.LittleEndian, hash.MaskCRC32(crc.Sum32())); err != nil {
		return err
	}

	t.off += len(buf) + 5
	return nil
}
