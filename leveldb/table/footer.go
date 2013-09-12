// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/syndtr/goleveldb/leveldb/errors"
)

// The magic was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
const xmagic uint64 = 0xdb4775248b80fb57

var magicBytes []byte

const (
	handlesSize = binary.MaxVarintLen64 * 2 * 2
	magicSize   = 8
	footerSize  = handlesSize + magicSize
)

func init() {
	magicBytes = make([]byte, magicSize)
	binary.LittleEndian.PutUint32(magicBytes, uint32(xmagic&0xffffffff))
	binary.LittleEndian.PutUint32(magicBytes[4:], uint32(xmagic>>32))
}

func writeFooter(w io.Writer, mi, ii *bInfo) (int, error) {
	buf := make([]byte, binary.MaxVarintLen64*2*2)
	i := mi.encodeTo(buf)
	ii.encodeTo(buf[i:])
	if _, err := w.Write(buf); err != nil {
		return 0, err
	}
	if _, err := w.Write(magicBytes); err != nil {
		return 0, err
	}
	return len(buf) + len(magicBytes), nil
}

func readFooter(r io.ReaderAt, size uint64) (mi, ii *bInfo, err error) {
	if size < uint64(footerSize) {
		return nil, nil, errors.ErrInvalid("file is too short to be an sstable")
	}

	buf := make([]byte, footerSize)
	n, err := r.ReadAt(buf, int64(size)-footerSize)
	if err != nil {
		return nil, nil, err
	}

	if bytes.Compare(buf[handlesSize:], magicBytes) != 0 {
		return nil, nil, errors.ErrInvalid("not an sstable (bad magic number)")
	}

	mi = new(bInfo)
	n, err = mi.decodeFrom(buf)
	if err != nil {
		return nil, nil, err
	}

	ii = new(bInfo)
	n, err = ii.decodeFrom(buf[n:])
	if err != nil {
		return nil, nil, err
	}

	return mi, ii, nil
}
