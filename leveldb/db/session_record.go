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
	"leveldb"
)

// These numbers are written to disk and should not be changed.
const (
	_ uint64 = iota
	tagComparer
	tagLogNum
	tagNextNum
	tagSeq
	tagCompactPointer
	tagDeletedTable
	tagNewTable
	// 8 was used for large value refs
	_
	tagPrevLogNum
)

const tagMax = tagPrevLogNum

var tagBytesCache [tagMax + 1][]byte

func init() {
	tmp := make([]byte, binary.MaxVarintLen32)
	for i := range tagBytesCache {
		n := binary.PutUvarint(tmp, uint64(i))
		b := make([]byte, n)
		copy(b, tmp)
		tagBytesCache[i] = b
	}
}

type cpRecord struct {
	level int
	key   iKey
}

type ntRecord struct {
	level int
	num   uint64
	size  uint64
	min   iKey
	max   iKey
}

func (r ntRecord) makeFile(s *session) *tFile {
	return newTFile(s.getTableFile(r.num), r.size, r.min, r.max)
}

type dtRecord struct {
	level int
	num   uint64
}

type sessionRecord struct {
	hasComparer bool
	comparer    string

	hasLogNum bool
	logNum    uint64

	hasNextNum bool
	nextNum    uint64

	hasSeq bool
	seq    uint64

	compactPointers []cpRecord
	newTables       []ntRecord
	deletedTables   []dtRecord
}

func (p *sessionRecord) setComparer(name string) {
	p.hasComparer = true
	p.comparer = name
}

func (p *sessionRecord) setLogNum(num uint64) {
	p.hasLogNum = true
	p.logNum = num
}

func (p *sessionRecord) setNextNum(num uint64) {
	p.hasNextNum = true
	p.nextNum = num
}

func (p *sessionRecord) setSeq(seq uint64) {
	p.hasSeq = true
	p.seq = seq
}

func (p *sessionRecord) addCompactPointer(level int, key iKey) {
	p.compactPointers = append(p.compactPointers, cpRecord{level, key})
}

func (p *sessionRecord) addTable(level int, num, size uint64, min, max iKey) {
	p.newTables = append(p.newTables, ntRecord{level, num, size, min, max})
}

func (p *sessionRecord) addTableFile(level int, t *tFile) {
	p.addTable(level, t.file.Number(), t.size, t.min, t.max)
}

func (p *sessionRecord) deleteTable(level int, num uint64) {
	p.deletedTables = append(p.deletedTables, dtRecord{level, num})
}

func (p *sessionRecord) encodeTo(w io.Writer) (err error) {
	tmp := make([]byte, binary.MaxVarintLen64)

	putUvarint := func(p uint64) (err error) {
		n := binary.PutUvarint(tmp, p)
		_, err = w.Write(tmp[:n])
		return
	}

	putBytes := func(p []byte) (err error) {
		err = putUvarint(uint64(len(p)))
		if err != nil {
			return
		}
		_, err = w.Write(p)
		if err != nil {
			return
		}
		return
	}

	if p.hasComparer {
		_, err = w.Write(tagBytesCache[tagComparer])
		if err != nil {
			return
		}
		err = putBytes([]byte(p.comparer))
		if err != nil {
			return
		}
	}

	if p.hasLogNum {
		_, err = w.Write(tagBytesCache[tagLogNum])
		if err != nil {
			return
		}
		err = putUvarint(p.logNum)
		if err != nil {
			return
		}
	}

	if p.hasNextNum {
		_, err = w.Write(tagBytesCache[tagNextNum])
		if err != nil {
			return
		}
		err = putUvarint(p.nextNum)
		if err != nil {
			return
		}
	}

	if p.hasSeq {
		_, err = w.Write(tagBytesCache[tagSeq])
		if err != nil {
			return
		}
		err = putUvarint(uint64(p.seq))
		if err != nil {
			return
		}
	}

	for _, p := range p.compactPointers {
		_, err = w.Write(tagBytesCache[tagCompactPointer])
		if err != nil {
			return
		}
		err = putUvarint(uint64(p.level))
		if err != nil {
			return
		}
		err = putBytes(p.key)
		if err != nil {
			return
		}
	}

	for _, p := range p.deletedTables {
		_, err = w.Write(tagBytesCache[tagDeletedTable])
		if err != nil {
			return
		}
		err = putUvarint(uint64(p.level))
		if err != nil {
			return
		}
		err = putUvarint(p.num)
		if err != nil {
			return
		}
	}

	for _, p := range p.newTables {
		_, err = w.Write(tagBytesCache[tagNewTable])
		if err != nil {
			return
		}
		err = putUvarint(uint64(p.level))
		if err != nil {
			return
		}
		err = putUvarint(p.num)
		if err != nil {
			return
		}
		err = putUvarint(p.size)
		if err != nil {
			return
		}
		err = putBytes(p.min)
		if err != nil {
			return
		}
		err = putBytes(p.max)
		if err != nil {
			return
		}
	}

	return
}

func (p *sessionRecord) encode() []byte {
	b := new(bytes.Buffer)
	p.encodeTo(b)
	return b.Bytes()
}

func (p *sessionRecord) decodeFrom(r readByteReader) (err error) {
	for err == nil {
		var tag uint64
		tag, err = binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}

		switch tag {
		case tagComparer:
			var cmp []byte
			cmp, err = readBytes(r)
			if err == nil {
				p.comparer = string(cmp)
				p.hasComparer = true
			}
		case tagLogNum:
			p.logNum, err = binary.ReadUvarint(r)
			if err == nil {
				p.hasLogNum = true
			}
		case tagPrevLogNum:
			err = leveldb.ErrInvalid("unsupported db format")
			break
		case tagNextNum:
			p.nextNum, err = binary.ReadUvarint(r)
			if err == nil {
				p.hasNextNum = true
			}
		case tagSeq:
			var seq uint64
			seq, err = binary.ReadUvarint(r)
			if err == nil {
				p.seq = seq
				p.hasSeq = true
			}
		case tagCompactPointer:
			var level uint64
			var b []byte
			level, err = binary.ReadUvarint(r)
			if err != nil {
				break
			}
			b, err = readBytes(r)
			if err != nil {
				break
			}
			p.addCompactPointer(int(level), b)
		case tagNewTable:
			var level, num, size uint64
			var b []byte
			level, err = binary.ReadUvarint(r)
			if err != nil {
				break
			}
			num, err = binary.ReadUvarint(r)
			if err != nil {
				break
			}
			size, err = binary.ReadUvarint(r)
			if err != nil {
				break
			}
			b, err = readBytes(r)
			if err != nil {
				break
			}
			min := iKey(b)
			b, err = readBytes(r)
			if err != nil {
				break
			}
			max := iKey(b)
			p.addTable(int(level), num, size, min, max)
		case tagDeletedTable:
			var level, num uint64
			level, err = binary.ReadUvarint(r)
			if err != nil {
				break
			}
			num, err = binary.ReadUvarint(r)
			if err != nil {
				break
			}
			p.deleteTable(int(level), num)
		}
	}

	return
}

func (p *sessionRecord) decode(buf []byte) error {
	b := bytes.NewBuffer(buf)
	return p.decodeFrom(b)
}
