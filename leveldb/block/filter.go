// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package block

import (
	"bytes"
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
)

var (
	// Generate new filter every 2KB of data
	filterBaseLg byte = 11
	filterBase   int  = 1 << filterBaseLg
)

// FilterWriter represent filter block writer.
type FilterWriter struct {
	filter filter.Filter

	buf     *bytes.Buffer
	keys    [][]byte
	offsets []uint32
}

// NewFilterWriter create new initialized filter block writer.
func NewFilterWriter(filter filter.Filter) *FilterWriter {
	return &FilterWriter{
		filter: filter,
		buf:    new(bytes.Buffer),
	}
}

// Generate generate filter up to given offset.
func (b *FilterWriter) Generate(offset int) {
	idx := offset / filterBase
	for idx > len(b.offsets) {
		b.generateFilter()
	}
}

// Add add key to the filter block.
func (b *FilterWriter) Add(key []byte) {
	b.keys = append(b.keys, key)
}

// Finish finalize the filter block.
func (b *FilterWriter) Finish() []byte {
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

func (b *FilterWriter) generateFilter() {
	b.offsets = append(b.offsets, uint32(b.buf.Len()))

	// fast path if there are no keys for this filter
	if len(b.keys) == 0 {
		return
	}

	// generate filter for current set of keys and append to buffer
	b.filter.CreateFilter(b.keys, b.buf)

	b.keys = nil
}

// FilterReader represent a filter block reader.
type FilterReader struct {
	filter filter.Filter
	buf    []byte

	baseLg       uint
	offsetsStart uint32
	length       uint

	ob []byte // offset buffer
}

// NewFilterReader create new initialized filter block reader.
func NewFilterReader(buf []byte, filter filter.Filter) (f *FilterReader, err error) {
	// 4 bytes for offset start and 1 byte for baseLg
	if len(buf) < 5 {
		err = errors.ErrCorrupt("filter block to short")
		return
	}

	offsetsStart := binary.LittleEndian.Uint32(buf[len(buf)-5:])
	if offsetsStart > uint32(len(buf))-5 {
		err = errors.ErrCorrupt("bad restart offset in filter block")
		return
	}

	f = &FilterReader{
		filter:       filter,
		buf:          buf,
		baseLg:       uint(buf[len(buf)-1]),
		offsetsStart: offsetsStart,
		length:       (uint(len(buf)) - 5 - uint(offsetsStart)) / 4,
		ob:           buf[offsetsStart : len(buf)-1],
	}
	return
}

// KeyMayMatch test whether given key at given offset may match.
func (b *FilterReader) KeyMayMatch(offset uint, key []byte) bool {
	idx := offset >> b.baseLg
	if idx < b.length {
		ob := b.ob[idx*4:]
		start := binary.LittleEndian.Uint32(ob)
		end := binary.LittleEndian.Uint32(ob[4:])
		if start <= end && end <= b.offsetsStart {
			filter := b.buf[start:end]
			return b.filter.KeyMayMatch(key, filter)
		} else if start == end {
			// Empty filters do not match any keys
			return false
		}
	}
	// Errors are treated as potential matches
	return true
}
