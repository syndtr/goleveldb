// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package block

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
)

var (
	// Generate new filter every 2KB of data
	filterBaseLg byte = 11
	filterBase   int  = 1 << filterBaseLg
)

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
			return b.filter.Contains(filter, key)
		} else if start == end {
			// Empty filters do not match any keys
			return false
		}
	}
	// Errors are treated as potential matches
	return true
}
