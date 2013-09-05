// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"encoding/binary"
	"io"
)

// These numbers are written to disk and should not be changed.
const (
	tagComparer       = 1
	tagJournalNum     = 2
	tagNextNum        = 3
	tagSeq            = 4
	tagCompactPointer = 5
	tagDeletedTable   = 6
	tagNewTable       = 7
	// 8 was used for large value refs
	tagPrevJournalNum = 9
)

const tagMax = tagPrevJournalNum

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

	hasJournalNum bool
	journalNum    uint64

	hasPrevJournalNum bool
	prevJournalNum    uint64

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

func (p *sessionRecord) setJournalNum(num uint64) {
	p.hasJournalNum = true
	p.journalNum = num
}

func (p *sessionRecord) setPrevJournalNum(num uint64) {
	p.hasPrevJournalNum = true
	p.prevJournalNum = num
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
	p.addTable(level, t.file.Num(), t.size, t.min, t.max)
}

func (p *sessionRecord) deleteTable(level int, num uint64) {
	p.deletedTables = append(p.deletedTables, dtRecord{level, num})
}

func (p *sessionRecord) encodeTo(w io.Writer) error {
	tmp := make([]byte, binary.MaxVarintLen64)

	putUvarint := func(p uint64) error {
		n := binary.PutUvarint(tmp, p)
		_, err := w.Write(tmp[:n])
		return err
	}

	putBytes := func(p []byte) error {
		if err := putUvarint(uint64(len(p))); err != nil {
			return err
		}
		if _, err := w.Write(p); err != nil {
			return err
		}
		return nil
	}

	if p.hasComparer {
		if _, err := w.Write(tagBytesCache[tagComparer]); err != nil {
			return err
		}
		if err := putBytes([]byte(p.comparer)); err != nil {
			return err
		}
	}

	if p.hasJournalNum {
		if _, err := w.Write(tagBytesCache[tagJournalNum]); err != nil {
			return err
		}
		if err := putUvarint(p.journalNum); err != nil {
			return err
		}
	}

	if p.hasNextNum {
		if _, err := w.Write(tagBytesCache[tagNextNum]); err != nil {
			return err
		}
		if err := putUvarint(p.nextNum); err != nil {
			return err
		}
	}

	if p.hasSeq {
		if _, err := w.Write(tagBytesCache[tagSeq]); err != nil {
			return err
		}
		if err := putUvarint(uint64(p.seq)); err != nil {
			return err
		}
	}

	for _, p := range p.compactPointers {
		if _, err := w.Write(tagBytesCache[tagCompactPointer]); err != nil {
			return err
		}
		if err := putUvarint(uint64(p.level)); err != nil {
			return err
		}
		if err := putBytes(p.key); err != nil {
			return err
		}
	}

	for _, p := range p.deletedTables {
		if _, err := w.Write(tagBytesCache[tagDeletedTable]); err != nil {
			return err
		}
		if err := putUvarint(uint64(p.level)); err != nil {
			return err
		}
		if err := putUvarint(p.num); err != nil {
			return err
		}
	}

	for _, p := range p.newTables {
		if _, err := w.Write(tagBytesCache[tagNewTable]); err != nil {
			return err
		}
		if err := putUvarint(uint64(p.level)); err != nil {
			return err
		}
		if err := putUvarint(p.num); err != nil {
			return err
		}
		if err := putUvarint(p.size); err != nil {
			return err
		}
		if err := putBytes(p.min); err != nil {
			return err
		}
		if err := putBytes(p.max); err != nil {
			return err
		}
	}

	return nil
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
			return err
		}

		switch tag {
		case tagComparer:
			var cmp []byte
			cmp, err = readBytes(r)
			if err == nil {
				p.comparer = string(cmp)
				p.hasComparer = true
			}
		case tagJournalNum:
			p.journalNum, err = binary.ReadUvarint(r)
			if err == nil {
				p.hasJournalNum = true
			}
		case tagPrevJournalNum:
			p.prevJournalNum, err = binary.ReadUvarint(r)
			if err == nil {
				p.hasPrevJournalNum = true
			}
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

	return err
}

func (p *sessionRecord) decode(buf []byte) error {
	b := bytes.NewBuffer(buf)
	return p.decodeFrom(b)
}
