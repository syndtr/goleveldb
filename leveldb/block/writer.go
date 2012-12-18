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

type Writer struct {
	restartInterval int

	buf      *bytes.Buffer
	restarts []uint32
	lastKey  []byte
	n        int
	closed   bool
}

func NewWriter(restartInterval int) *Writer {
	return &Writer{
		restartInterval: restartInterval,
		buf:             new(bytes.Buffer),
	}
}

func (b *Writer) Add(key, value []byte) {
	if b.closed {
		panic("opeartion on closed block writer")
	}

	// Get key shared prefix
	shared := 0
	if b.n%b.restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(b.buf.Len()))
	} else {
		n := len(b.lastKey)
		if n > len(key) {
			n = len(key)
		}
		for shared < n && b.lastKey[shared] == key[shared] {
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

	b.lastKey = key
	b.n++
}

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

func (b *Writer) Reset() {
	b.buf.Reset()
	b.restarts = nil
	b.lastKey = nil
	b.n = 0
	b.closed = false
}

func (b *Writer) Len() int {
	return b.n
}

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

func (b *Writer) CountRestart() int {
	return len(b.restarts)
}
