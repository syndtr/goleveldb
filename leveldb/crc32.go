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

package leveldb

import (
	"hash"
	"hash/crc32"
)

var crc32tab = crc32.MakeTable(crc32.Castagnoli)

func NewCRC32C() hash.Hash32 {
	return crc32.New(crc32tab)
}

var crcMaskDelta uint32 = 0xa282ead8

// Return a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs.  Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
func MaskCRC32(crc uint32) uint32 {
	// Rotate right by 15 bits and add a constant.
	return ((crc >> 15) | (crc << 17)) + crcMaskDelta
}

// Return the crc whose masked representation is masked_crc.
func UnmaskCRC32(masked_crc uint32) uint32 {
	rot := masked_crc - crcMaskDelta
	return ((rot >> 17) | (rot << 15))
}
