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

package log

import (
	"bytes"
	"encoding/binary"
	"io"
	"leveldb"
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
	kBlockSize = 32768

	// Header is checksum (4 bytes), type (1 byte), length (2 bytes).
	kHeaderSize = 4 + 1 + 2
)

var sixZero [6]byte

type Writer struct {
	w io.Writer

	boff int
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (l *Writer) Append(record []byte) (err error) {
	begin := true
	for {
		leftover := kBlockSize - l.boff
		if leftover < kHeaderSize {
			// Switch to a new block
			if leftover > 0 {
				_, err = l.w.Write(sixZero[:leftover])
				if err != nil {
					return
				}
			}
			l.boff = 0
		}

		avail := kBlockSize - l.boff - kHeaderSize
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

		err = l.write(rtype, record[:fragLen])
		if err != nil {
			return
		}

		record = record[fragLen:]
		begin = false

		l.boff += kHeaderSize + fragLen

		if len(record) <= 0 {
			break
		}
	}

	return
}

func (l *Writer) write(rtype uint, record []byte) (err error) {
	rlen := len(record)
	buf := new(bytes.Buffer)

	crc := leveldb.NewCRC32C()
	crc.Write([]byte{byte(rtype)})
	crc.Write(record)
	err = binary.Write(buf, binary.LittleEndian, leveldb.MaskCRC32(crc.Sum32()))
	if err != nil {
		return
	}

	buf.WriteByte(byte(rlen & 0xff))
	buf.WriteByte(byte(rlen >> 8))
	buf.WriteByte(byte(rtype))

	_, err = l.w.Write(buf.Bytes())
	if err != nil {
		return
	}
	_, err = l.w.Write(record)
	if err != nil {
		return
	}

	return
}
