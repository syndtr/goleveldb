// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmaiw.com>
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

package journal

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/syndtr/goleveldb/leveldb/hash"
)

const (
	// Zero is reserved for preallocated files
	tZero uint = iota
	tFull
	tFirst
	tMiddle
	tLast

	// Internal use
	tCorrupt
	tEof
)

const (
	// Journal block size.
	BlockSize = 32768

	// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
	kHeaderSize = 4 + 2 + 1
)

var sixZero [6]byte

// Writer represent a journal writer.
type Writer struct {
	w   io.Writer
	buf bytes.Buffer

	boff int
}

// NewWriter create new initialized journal writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Append append record to the journal.
func (w *Writer) Append(record []byte) (err error) {
	begin := true
	for {
		leftover := BlockSize - w.boff
		if leftover < kHeaderSize {
			// Switch to a new block
			if leftover > 0 {
				_, err = w.w.Write(sixZero[:leftover])
				if err != nil {
					return
				}
			}
			w.boff = 0
		}

		avail := BlockSize - w.boff - kHeaderSize
		fragLen := len(record)
		end := true
		if fragLen > avail {
			fragLen = avail
			end = false
		}

		rtype := tMiddle
		if begin && end {
			rtype = tFull
		} else if begin {
			rtype = tFirst
		} else if end {
			rtype = tLast
		}

		err = w.write(rtype, record[:fragLen])
		if err != nil {
			return
		}

		record = record[fragLen:]
		begin = false

		w.boff += kHeaderSize + fragLen

		if len(record) <= 0 {
			break
		}
	}
	return
}

func (w *Writer) write(rtype uint, record []byte) (err error) {
	rlen := len(record)
	buf := &w.buf
	buf.Reset()

	crc := hash.NewCRC32C()
	crc.Write([]byte{byte(rtype)})
	crc.Write(record)
	binary.Write(buf, binary.LittleEndian, hash.MaskCRC32(crc.Sum32()))

	buf.WriteByte(byte(rlen & 0xff))
	buf.WriteByte(byte(rlen >> 8))
	buf.WriteByte(byte(rtype))

	_, err = buf.WriteTo(w.w)
	if err == nil {
		_, err = w.w.Write(record)
	}
	return err
}
