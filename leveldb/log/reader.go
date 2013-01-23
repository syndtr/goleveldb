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

// Package log allows read and write sequence of data block.
package log

import (
	"bytes"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb/hash"
	"io"
)

type DropFunc func(n int, reason string)

// Reader represent a log reader.
type Reader struct {
	r        io.ReaderAt
	checksum bool
	dropf    DropFunc

	eof       bool
	rbuf, buf []byte
	off       int
	record    []byte
	err       error
}

// NewReader create new initialized log reader.
func NewReader(r io.ReaderAt, checksum bool, dropf DropFunc) *Reader {
	return &Reader{
		r:        r,
		checksum: checksum,
		dropf:    dropf,
	}
}

// Skip allow skip given number bytes, rounded by single block.
func (l *Reader) Skip(skip int) error {
	if skip > 0 {
		l.off = skip / BlockSize
		if skip%BlockSize > 0 {
			l.off++
		}
	}
	return nil
}

func (l *Reader) drop(n int, reason string) {
	if l.dropf != nil {
		l.dropf(n, reason)
	}
}

// Next read the next return, return true if there is next record,
// otherwise return false.
func (l *Reader) Next() bool {
	if l.err != nil {
		return false
	}

	l.record = nil

	inFragment := false
	buf := new(bytes.Buffer)
	for {
		var rec []byte
		var rtype uint
		rec, rtype, l.err = l.read()
		if l.err != nil {
			return false
		}

		switch rtype {
		case tFull:
			if inFragment {
				l.drop(buf.Len(), "partial record without end; tag=full")
				buf.Reset()
			}
			buf.Write(rec)
			l.record = buf.Bytes()
			return true
		case tFirst:
			if inFragment {
				l.drop(buf.Len(), "partial record without end; tag=first")
				buf.Reset()
			}
			buf.Write(rec)
			inFragment = true
		case tMiddle:
			if inFragment {
				buf.Write(rec)
			} else {
				l.drop(len(rec), "missing start of fragmented record; tag=mid")
			}
		case tLast:
			if inFragment {
				buf.Write(rec)
				l.record = buf.Bytes()
				return true
			} else {
				l.drop(len(rec), "missing start of fragmented record; tag=last")
			}
		case tEof:
			if inFragment {
				l.drop(buf.Len(), "partial record without end; tag=eof")
			}
			return false
		case tCorrupt:
			if inFragment {
				l.drop(buf.Len(), "record fragment corrupted")
				buf.Reset()
				inFragment = false
			}
		}
	}
	return false
}

// Record return current record.
func (l *Reader) Record() []byte {
	return l.record
}

// Error return any record produced by previous operation.
func (l *Reader) Error() error {
	return l.err
}

func (l *Reader) read() (ret []byte, rtype uint, err error) {
retry:
	if len(l.buf) < kHeaderSize {
		if l.eof {
			if len(l.buf) > 0 {
				l.drop(len(l.buf), "truncated record at end of file")
				l.rbuf = nil
				l.buf = nil
			}
			rtype = tEof
			return
		}

		if l.rbuf == nil {
			l.rbuf = make([]byte, BlockSize)
		} else {
			l.off++
		}

		var n int
		n, err = l.r.ReadAt(l.rbuf, int64(l.off)*BlockSize)
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				return
			}
		}
		l.buf = l.rbuf[:n]
		if n < BlockSize {
			l.eof = true
			goto retry
		}
	}

	// decode record length and type
	recLen := int(l.buf[4]) | (int(l.buf[5]) << 8)
	rtype = uint(l.buf[6])

	// check whether the header is sane
	if len(l.buf) < kHeaderSize+recLen || rtype > tLast {
		rtype = tCorrupt
		l.drop(len(l.buf), "header corrupted")
	} else if l.checksum {
		// decode the checksum
		recCrc := hash.UnmaskCRC32(binary.LittleEndian.Uint32(l.buf))
		crc := hash.NewCRC32C()
		crc.Write(l.buf[6 : kHeaderSize+recLen])
		if crc.Sum32() != recCrc {
			// Drop the rest of the buffer since "length" itself may have
			// been corrupted and if we trust it, we could find some
			// fragment of a real log record that just happens to look
			// like a valid log record.
			rtype = tCorrupt
			l.drop(len(l.buf), "checksum mismatch")
		}
	}

	if rtype == tCorrupt {
		// report bytes drop
		l.buf = nil
	} else {
		ret = l.buf[kHeaderSize : kHeaderSize+recLen]
		l.buf = l.buf[kHeaderSize+recLen:]
	}

	return
}
