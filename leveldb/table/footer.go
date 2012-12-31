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
	"io"
	"leveldb/errors"
)

// The magic was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
const magic uint64 = 0xdb4775248b80fb57

var magicBytes []byte

const (
	handlesSize = binary.MaxVarintLen64 * 2 * 2
	magicSize   = 8
	footerSize  = handlesSize + magicSize
)

func init() {
	magicBytes = make([]byte, magicSize)
	binary.LittleEndian.PutUint32(magicBytes, uint32(magic&0xffffffff))
	binary.LittleEndian.PutUint32(magicBytes[4:], uint32(magic>>32))
}

func writeFooter(w io.Writer, mi, ii *bInfo) (n int, err error) {
	buf := make([]byte, binary.MaxVarintLen64*2*2)
	i := mi.encodeTo(buf)
	ii.encodeTo(buf[i:])
	_, err = w.Write(buf)
	if err != nil {
		return
	}
	_, err = w.Write(magicBytes)
	if err != nil {
		return
	}
	return len(buf) + len(magicBytes), nil
}

func readFooter(r io.ReaderAt, size uint64) (mi, ii *bInfo, err error) {
	if size < uint64(footerSize) {
		err = errors.ErrInvalid("file is too short to be an sstable")
		return
	}

	buf := make([]byte, footerSize)
	n, err := r.ReadAt(buf, int64(size)-footerSize)
	if err != nil {
		return
	}

	if bytes.Compare(buf[handlesSize:], magicBytes) != 0 {
		err = errors.ErrInvalid("not an sstable (bad magic number)")
		return
	}

	mi = new(bInfo)
	n, err = mi.decodeFrom(buf)
	if err != nil {
		return
	}

	ii = new(bInfo)
	n, err = ii.decodeFrom(buf[n:])
	if err != nil {
		return
	}

	return
}
