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

	"snappy"

	"leveldb/errors"
	"leveldb/hash"
	"leveldb/opt"
)

// bInfo holds information about where and how long a block is
type bInfo struct {
	offset, size uint64
}

func (p *bInfo) decodeFrom(b []byte) (int, error) {
	var n, m int
	p.offset, n = binary.Uvarint(b)
	if n > 0 {
		p.size, m = binary.Uvarint(b[n:])
	}
	if n <= 0 || m <= 0 {
		return 0, errors.ErrCorrupt("bad block handle")
	}
	return n + m, nil
}

// Encode encode bInfo, bInfo encoded into varints
func (p *bInfo) encode() []byte {
	b := make([]byte, binary.MaxVarintLen64*2)
	return b[:p.encodeTo(b)]
}

func (p *bInfo) encodeTo(b []byte) int {
	n := binary.PutUvarint(b, p.offset)
	m := binary.PutUvarint(b[n:], p.size)
	return n + m
}

func readFullAt(r io.ReaderAt, buf []byte, off int64) (n int, err error) {
	for n < len(buf) && err == nil {
		var nn int
		nn, err = r.ReadAt(buf[n:], off+int64(n))
		n += nn
	}
	if err == io.EOF {
		if n >= len(buf) {
			err = nil
		} else if n > 0 {
			err = io.ErrUnexpectedEOF
		}
	}
	return
}

// readAll read entire referenced block.
func (p *bInfo) readAll(r io.ReaderAt, checksum bool) (b []byte, err error) {
	raw := make([]byte, p.size+5)
	_, err = readFullAt(r, raw, int64(p.offset))
	if err != nil {
		return
	}

	crcb := raw[len(raw)-4:]
	raw = raw[:len(raw)-4]

	if checksum {
		sum := binary.LittleEndian.Uint32(crcb)
		sum = hash.UnmaskCRC32(sum)
		crc := hash.NewCRC32C()
		crc.Write(raw)
		if crc.Sum32() != sum {
			err = errors.ErrCorrupt("block checksum mismatch")
			return
		}
	}

	compression := opt.Compression(raw[len(raw)-1])
	b = raw[:len(raw)-1]

	switch compression {
	case opt.SnappyCompression:
		return snappy.Decode(nil, b)
	}

	return
}
