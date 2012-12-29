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

package table

import (
	"encoding/binary"
	"leveldb"
	"leveldb/block"
	"leveldb/descriptor"
)

type Writer struct {
	w   descriptor.Writer
	o   leveldb.OptionsInterface
	cmp leveldb.Comparator

	dataBlock   *block.Writer
	indexBlock  *block.Writer
	filterBlock *block.FilterWriter

	n, offset    int
	lastKey      []byte
	pendingIndex bool
	pendingBlock *bInfo

	closed bool
}

func NewWriter(w descriptor.Writer, o leveldb.OptionsInterface) *Writer {
	t := &Writer{w: w, o: o, cmp: o.GetComparator()}
	// Creating blocks
	t.dataBlock = block.NewWriter(o.GetBlockRestartInterval())
	t.indexBlock = block.NewWriter(1)
	filterPolicy := o.GetFilter()
	if filterPolicy != nil {
		t.filterBlock = block.NewFilterWriter(filterPolicy)
		t.filterBlock.StartBlock(0)
	}
	t.pendingBlock = new(bInfo)
	return t
}

func (t *Writer) Add(key, value []byte) (err error) {
	if t.closed {
		panic("operation on closed table writer")
	}

	if t.pendingIndex {
		sep := t.cmp.FindShortestSeparator(t.lastKey, key)
		t.indexBlock.Add(sep, t.pendingBlock.encode())
		t.pendingIndex = false
	}

	if t.filterBlock != nil {
		t.filterBlock.AddKey(key)
	}

	t.lastKey = key
	t.n++

	t.dataBlock.Add(key, value)
	if t.dataBlock.Size() >= t.o.GetBlockSize() {
		err = t.Flush()
	}
	return
}

func (t *Writer) Flush() (err error) {
	if t.closed {
		panic("operation on closed table writer")
	}

	if t.pendingIndex {
		return
	}

	err = t.write(t.dataBlock.Finish(), t.pendingBlock, false)
	if err != nil {
		return
	}
	t.dataBlock.Reset()

	t.pendingIndex = true

	if t.filterBlock != nil {
		t.filterBlock.StartBlock(t.offset)
	}
	return
}

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
	if t.filterBlock != nil {
		err = t.write(t.filterBlock.Finish(), fi, true)
		if err != nil {
			return
		}
	}

	// Write meta block
	metaBlock := block.NewWriter(t.o.GetBlockRestartInterval())
	if t.filterBlock != nil {
		filterPolicy := t.o.GetFilter()
		key := []byte("filter." + filterPolicy.Name())
		metaBlock.Add(key, fi.encode())
	}
	mi := new(bInfo)
	err = t.write(metaBlock.Finish(), mi, false)
	if err != nil {
		return
	}

	// Write index block
	if t.pendingIndex {
		suc := t.cmp.FindShortSuccessor(t.lastKey)
		t.indexBlock.Add(suc, t.pendingBlock.encode())
		t.pendingIndex = false
	}
	ii := new(bInfo)
	err = t.write(t.indexBlock.Finish(), ii, false)
	if err != nil {
		return
	}

	// Write footer
	var n int
	n, err = writeFooter(t.w, mi, ii)
	if err != nil {
		return
	}
	t.offset += n

	return
}

func (t *Writer) Len() int {
	return t.n
}

func (t *Writer) Size() int {
	return t.offset
}

func (t *Writer) CountBlock() int {
	n := t.indexBlock.Len()
	if !t.closed {
		n++
	}
	return n
}

func (t *Writer) write(buf []byte, bi *bInfo, raw bool) (err error) {
	compression := leveldb.NoCompression
	if !raw {
		compression = t.o.GetCompressionType()
	}
	switch compression {
	case leveldb.SnappyCompression:
		compression = leveldb.NoCompression
	}

	if bi != nil {
		bi.offset = uint64(t.offset)
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

	crc := leveldb.NewCRC32C()
	crc.Write(buf)
	crc.Write(compbit)
	err = binary.Write(t.w, binary.LittleEndian, leveldb.MaskCRC32(crc.Sum32()))
	if err != nil {
		return
	}

	t.offset += len(buf) + 5
	return
}
