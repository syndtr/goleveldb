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
	"io"
	"leveldb"
)

type Writer interface {
	io.Writer
	leveldb.Syncer
}

type Builder struct {
	w Writer
	o *leveldb.Options

	dataBlock   *blockBuilder
	indexBlock  *blockBuilder
	filterBlock *filterBlockBuilder

	n, offset     int
	lastKey       []byte
	pendingIndex  bool
	pendingHandle *blockHandle

	closed bool
}

func NewBuilder(w Writer, o *leveldb.Options) *Builder {
	t := &Builder{w: w}
	// Copy options
	t.o = new(leveldb.Options)
	*t.o = *o
	// Creating blocks
	t.dataBlock = newBlockBuilder(o, 0)
	t.indexBlock = newBlockBuilder(o, 1)
	filterPolicy := o.GetFilterPolicy()
	if filterPolicy != nil {
		t.filterBlock = newFilterBlockBuilder(filterPolicy)
		t.filterBlock.StartBlock(0)
	}
	t.pendingHandle = new(blockHandle)
	return t
}

func (t *Builder) Add(key, value []byte) (err error) {
	if t.closed {
		panic("operation on closed table builder")
	}

	cmp := t.o.GetComparator()
	if t.n > 0 && cmp.Compare(key, t.lastKey) <= 0 {
		panic("the key preceding last added key")
	}

	if t.pendingIndex {
		sep := cmp.FindShortestSeparator(t.lastKey, key)
		t.indexBlock.Add(sep, t.pendingHandle.Encode())
		t.pendingIndex = false
	}

	if t.filterBlock != nil {
		t.filterBlock.AddKey(key)
	}

	t.lastKey = key
	t.n++

	t.dataBlock.Add(key, value)
	if t.dataBlock.Len() >= t.o.GetBlockSize() {
		err = t.Flush()
	}
	return
}

func (t *Builder) Flush() (err error) {
	if t.closed {
		panic("operation on closed table builder")
	}

	if t.pendingIndex {
		return
	}

	err = t.write(t.dataBlock.Finish(), t.pendingHandle, false)
	if err != nil {
		return
	}
	t.dataBlock.Reset()

	err = t.w.Sync()
	if err != nil {
		return
	}

	t.pendingIndex = true

	if t.filterBlock != nil {
		t.filterBlock.StartBlock(t.offset)
	}
	return
}

func (t *Builder) Finish() (err error) {
	if t.closed {
		panic("operation on closed table builder")
	}

	err = t.Flush()
	if err != nil {
		return
	}

	t.closed = true

	var filterHandle, metaHandle, indexHandle *blockHandle

	// Write filter block
	if t.filterBlock != nil {
		filterHandle = new(blockHandle)
		err = t.write(t.filterBlock.Finish(), filterHandle, true)
		if err != nil {
			return
		}
	}

	// Write meta block
	metaBlock := newBlockBuilder(t.o, 0)
	metaHandle = new(blockHandle)
	if t.filterBlock != nil {
		filterPolicy := t.o.GetFilterPolicy()
		key := []byte("filter." + filterPolicy.Name())
		metaBlock.Add(key, filterHandle.Encode())
	}
	err = t.write(metaBlock.Finish(), metaHandle, false)
	if err != nil {
		return
	}

	// Write index block
	if t.pendingIndex {
		cmp := t.o.GetComparator()
		suc := cmp.FindShortSuccessor(t.lastKey)
		t.indexBlock.Add(suc, t.pendingHandle.Encode())
		t.pendingIndex = false
	}
	indexHandle = new(blockHandle)
	err = t.write(t.indexBlock.Finish(), indexHandle, false)
	if err != nil {
		return
	}

	// Write footer
	var n int
	n, err = writeFooter(t.w, metaHandle, indexHandle)
	if err != nil {
		return
	}
	t.offset += n
	return
}

func (t *Builder) NumEntries() int {
	return t.n
}

func (t *Builder) FileSize() int {
	return t.offset
}

func (t *Builder) write(buf []byte, handle *blockHandle, raw bool) (err error) {
	compression := leveldb.NoCompression
	if !raw {
		compression = t.o.GetCompressionType()
	}
	switch compression {
	case leveldb.SnappyCompression:
		compression = leveldb.NoCompression
	}

	if handle != nil {
		handle.Offset = uint64(t.offset)
		handle.Size = uint64(len(buf))
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
