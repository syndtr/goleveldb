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
	"io"
	"os"
	"bytes"
	"encoding/binary"
	"leveldb"
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
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(magic & 0xffffffff))
	binary.Write(buf, binary.LittleEndian, uint32(magic >> 32))
	magicBytes = buf.Bytes()
}

func writeFooter(w io.Writer, metaHandle, indexHandle *blockHandle) (n int, err error) {
	buf := make([]byte, binary.MaxVarintLen64 * 2 * 2)
	i := metaHandle.EncodeTo(buf)
	indexHandle.EncodeTo(buf[i:])
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

func readFooter(r leveldb.Reader) (metaHandle, indexHandle *blockHandle, err error) {
	var fi os.FileInfo
	fi, err = r.Stat()
	if err != nil {
		return
	} else if fi.Size() < int64(footerSize) {
		err = leveldb.NewInvalidArgumentError("file is too short to be an sstable")
		return
	}

	var n int
	buf := make([]byte, footerSize)
	n, err = r.ReadAt(buf, fi.Size() - int64(footerSize))
	if err != nil {
		return
	}
	
	if bytes.Compare(buf[handlesSize:], magicBytes) != 0 {
		err = leveldb.NewInvalidArgumentError("not an sstable (bad magic number)")
		return
	}

	metaHandle = new(blockHandle)
	n, err = metaHandle.DecodeFrom(buf)
	if err != nil {
		return
	}

	indexHandle = new(blockHandle)
	n, err = indexHandle.DecodeFrom(buf[n:])
	if err != nil {
		return
	}

	return
}
