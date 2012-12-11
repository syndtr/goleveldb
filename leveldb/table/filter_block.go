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
	"bytes"
	"encoding/binary"
	"leveldb"
)

// Generate new filter every 2KB of data
var (
	filterBaseLg byte = 11
	filterBase   int  = 1 << filterBaseLg
)

type filterBlockBuilder struct {
	policy leveldb.FilterPolicy

	buf     *bytes.Buffer
	keys    [][]byte
	offsets []uint32
}

func newFilterBlockBuilder(policy leveldb.FilterPolicy) *filterBlockBuilder {
	return &filterBlockBuilder{
		policy: policy,
		buf:    new(bytes.Buffer),
	}
}

func (b *filterBlockBuilder) StartBlock(offset int) {
	idx := offset / filterBase
	for idx > len(b.offsets) {
		b.generateFilter()
	}
}

func (b *filterBlockBuilder) AddKey(key []byte) {
	b.keys = append(b.keys, key)
}

func (b *filterBlockBuilder) Finish() []byte {
	if len(b.keys) > 0 {
		b.generateFilter()
	}

	// Append array of per-filter offsets
	offsetsOffset := uint32(b.buf.Len())
	for _, offset := range b.offsets {
		binary.Write(b.buf, binary.LittleEndian, offset)
	}

	binary.Write(b.buf, binary.LittleEndian, offsetsOffset)
	b.buf.WriteByte(filterBaseLg)

	return b.buf.Bytes()
}

func (b *filterBlockBuilder) generateFilter() {
	b.offsets = append(b.offsets, uint32(b.buf.Len()))

	// fast path if there are no keys for this filter
	if len(b.keys) == 0 {
		return
	}

	// generate filter for current set of keys and append to buffer
	b.policy.CreateFilter(b.keys, b.buf)

	b.keys = nil
}

type filterBlock struct {
	policy leveldb.FilterPolicy
	buf    []byte

	baseLg       uint
	offsetsStart uint32
	length       uint

	or *bytes.Reader // offset reader
}

func newFilterBlock(policy leveldb.FilterPolicy, buf []byte) (b *filterBlock, err error) {
	if len(buf) < 5 {
		err = leveldb.ErrCorrupt("filter block to short")
		return
	}

	r := bytes.NewReader(buf)
	r.Seek(-5, 2)
	var offsetsStart uint32
	err = binary.Read(r, binary.LittleEndian, &offsetsStart)
	if err != nil {
		return
	}
	if offsetsStart > uint32(len(buf))-5 {
		err = leveldb.ErrCorrupt("bad restart offset in filter block")
		return
	}

	b = &filterBlock{
		policy:       policy,
		buf:          buf,
		baseLg:       uint(buf[len(buf)-1]),
		offsetsStart: offsetsStart,
		length:       (uint(len(buf)) - 5 - uint(offsetsStart)) / 4,
		or:           bytes.NewReader(buf[offsetsStart : len(buf)-1]),
	}
	return
}

func newFilterBlockFromHandle(handle *blockHandle, r leveldb.Reader, verify bool, policy leveldb.FilterPolicy) (b *filterBlock, err error) {
	var buf []byte
	buf, err = handle.ReadAll(r, verify)
	if err != nil {
		return
	}
	return newFilterBlock(policy, buf)
}

func (b *filterBlock) KeyMayMatch(offset uint, key []byte) bool {
	idx := offset >> b.baseLg
	if idx < b.length {
		var start, end uint32
		b.or.Seek(int64(idx)*4, 0)
		binary.Read(b.or, binary.LittleEndian, &start)
		binary.Read(b.or, binary.LittleEndian, &end)
		if start <= end && end <= b.offsetsStart {
			filter := b.buf[start:end]
			return b.policy.KeyMayMatch(key, filter)
		} else if start == end {
			// Empty filters do not match any keys
			return false
		}
	}
	// Errors are treated as potential matches
	return true
}
