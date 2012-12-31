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

package block

import (
	"bytes"
	"encoding/binary"
)

// Writer represent a block writer,
type Writer struct {
	restartInterval int

	buf      *bytes.Buffer
	restarts []uint32
	lkey     []byte
	n        int
	closed   bool
}

// NewWriter create new initialized block writer.
func NewWriter(restartInterval int) *Writer {
	return &Writer{
		restartInterval: restartInterval,
		buf:             new(bytes.Buffer),
	}
}

// Add append key/value to the block.
func (b *Writer) Add(key, value []byte) {
	if b.closed {
		panic("opeartion on closed block writer")
	}

	// Get key shared prefix
	shared := 0
	if b.n%b.restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(b.buf.Len()))
	} else {
		n := len(b.lkey)
		if n > len(key) {
			n = len(key)
		}
		for shared < n && b.lkey[shared] == key[shared] {
			shared++
		}
	}

	// Add "<shared><non_shared><value_size>" to buffer
	var n int
	var tmp = make([]byte, binary.MaxVarintLen32)
	n = binary.PutUvarint(tmp, uint64(shared))
	b.buf.Write(tmp[:n])
	n = binary.PutUvarint(tmp, uint64(len(key)-shared))
	b.buf.Write(tmp[:n])
	n = binary.PutUvarint(tmp, uint64(len(value)))
	b.buf.Write(tmp[:n])

	// Add string delta to buffer followed by value
	b.buf.Write(key[shared:])
	b.buf.Write(value)

	b.lkey = key
	b.n++
}

// Finish finalize the block. No Add() is possible beyond this
// unless after Reset(), or panic will raised.
func (b *Writer) Finish() []byte {
	if b.closed {
		panic("opeartion on closed block writer")
	}

	b.closed = true

	// C++ leveldb need atleast 1 restart entry
	if b.restarts == nil {
		b.restarts = make([]uint32, 1)
	}

	for _, restart := range b.restarts {
		binary.Write(b.buf, binary.LittleEndian, restart)
	}
	binary.Write(b.buf, binary.LittleEndian, uint32(len(b.restarts)))

	return b.buf.Bytes()
}

// Reset reset the states of this block writer.
func (b *Writer) Reset() {
	b.buf.Reset()
	b.restarts = nil
	b.lkey = nil
	b.n = 0
	b.closed = false
}

// Len return the sum of added entries.
func (b *Writer) Len() int {
	return b.n
}

// Size return actual size of block in bytes,
func (b *Writer) Size() int {
	n := b.buf.Len()
	if !b.closed {
		n += len(b.restarts)*4 + 4
		if b.restarts == nil {
			n += 4
		}
	}
	return n
}

// CountRestart return the number of restarts point.
func (b *Writer) CountRestart() int {
	return len(b.restarts)
}
