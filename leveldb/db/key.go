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

package db

import (
	"bytes"
	"encoding/binary"
	"io"
)

type vType int

const (
	tDel vType = iota
	tVal
)

const tSeek = tVal

const kMaxSeq uint64 = (uint64(1) << 56) - 1

func packSequenceAndType(seq uint64, t vType) uint64 {
	if seq > kMaxSeq || t > tVal {
		panic("invalid sequence number or value type")
	}
	return (uint64(seq) << 8) | uint64(t)
}

func unpackSequenceAndType(packed uint64) (uint64, vType) {
	return uint64(packed >> 8), vType(packed & 0xff)
}

type parsedIKey struct {
	ukey     []byte
	sequence uint64
	vtype    vType
}

type iKey []byte

func writeIkey(w io.Writer, ukey []byte, seq uint64, t vType) {
	w.Write(ukey)
	binary.Write(w, binary.LittleEndian, packSequenceAndType(seq, t))
}

func newIKey(ukey []byte, seq uint64, t vType) iKey {
	b := new(bytes.Buffer)
	writeIkey(b, ukey, seq, t)
	return b.Bytes()
}

func newIKeyFromParsed(k *parsedIKey) iKey {
	return newIKey(k.ukey, k.sequence, k.vtype)
}

func (p iKey) ukey() []byte {
	if p == nil {
		panic("operation on nil iKey")
	}
	return p[:len(p)-8]
}

func (p iKey) sequenceAndType() (valid bool, seq uint64, t vType) {
	if p == nil {
		panic("operation on nil iKey")
	}
	if len(p) < 8 {
		return false, 0, 0
	}
	packed := binary.LittleEndian.Uint64(p[len(p)-8:])
	seq, t = unpackSequenceAndType(packed)
	if t > tVal {
		return false, 0, 0
	}
	valid = true
	return
}

func (p iKey) parse() *parsedIKey {
	valid, seq, t := p.sequenceAndType()
	if !valid {
		return nil
	}
	return &parsedIKey{p.ukey(), seq, t}
}
