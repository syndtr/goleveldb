// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"io"

	"code.google.com/p/snappy-go/snappy"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/hash"
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

// readAll read entire referenced block.
func (p *bInfo) readAll(r io.ReaderAt, checksum bool) ([]byte, error) {
	raw := make([]byte, p.size+5)
	if _, err := r.ReadAt(raw, int64(p.offset)); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	crcb := raw[len(raw)-4:]
	raw = raw[:len(raw)-4]

	if checksum {
		sum := binary.LittleEndian.Uint32(crcb)
		if hash.NewCRC(raw).Value() != sum {
			return nil, errors.ErrCorrupt("block checksum mismatch")
		}
	}

	compression := raw[len(raw)-1]
	b := raw[:len(raw)-1]

	switch compression {
	case blockTypeNoCompression:
	case blockTypeSnappyCompression:
		return snappy.Decode(nil, b)
	default:
		return nil, errors.ErrCorrupt("bad block type")
	}

	return b, nil
}
