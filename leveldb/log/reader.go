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

package log

import (
	"bytes"
	"encoding/binary"
	"io"
	"leveldb"
)

type Reader struct {
	r        io.ReadSeeker
	checksum bool

	bleft  int
	record []byte
	err    error
}

func NewReader(r io.ReadSeeker, checksum bool) *Reader {
	return &Reader{
		r:        r,
		checksum: checksum,
		bleft:    kBlockSize,
	}
}

func (l *Reader) Skip(skip int) error {
	if skip > 0 {
		rest := skip % kBlockSize
		blockOffset := skip - rest
		if rest > kBlockSize-(kHeaderSize-1) {
			rest = 0
			blockOffset += kBlockSize
		}
		if blockOffset > 0 {
			_, err := l.r.Seek(int64(blockOffset), 0)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

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
				// report bytes drop
			}
			l.record = rec
			return true
		case tFirst:
			if inFragment {
				// report bytes drop
				buf.Reset()
			}
			buf.Write(rec)
			inFragment = true
		case tMiddle:
			if inFragment {
				buf.Write(rec)
			} else {
				// report bytes drop
			}
		case tLast:
			if inFragment {
				buf.Write(rec)
				l.record = buf.Bytes()
				return true
			} else {
				// report bytes drop
			}
		case tEof:
			if inFragment {
				// report bytes drop
			}
			return false
		case tCorrupt:
			if inFragment {
				// report bytes drop
				buf.Reset()
				inFragment = false
			}
		}
	}
	return false
}

func (l *Reader) Record() []byte {
	return l.record
}

func (l *Reader) Error() error {
	return l.err
}

func (l *Reader) read() (ret []byte, rtype uint, err error) {
	defer func() {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = nil
			rtype = tEof
		}
	}()

	if l.bleft < kHeaderSize {
		if l.bleft > 0 {
			_, err = l.r.Seek(int64(l.bleft), 1)
			if err != nil {
				return
			}
		}
		l.bleft = kBlockSize
	}

	// decode the checksum
	var recCrc uint32
	if l.checksum {
		err = binary.Read(l.r, binary.LittleEndian, &recCrc)
		if err != nil {
			return
		}
		recCrc = leveldb.UnmaskCRC32(recCrc)
	} else {
		_, err = l.r.Seek(4, 0)
		if err != nil {
			return
		}
	}

	// decode record length and type
	var header [3]byte
	_, err = io.ReadFull(l.r, header[:])
	if err != nil {
		return
	}
	recLen := int(header[0]) | (int(header[1]) << 8)
	rtype = uint(header[2])

	l.bleft -= kHeaderSize

	// check wether the header is sane
	if l.bleft < recLen || rtype > tLast {
		// skip entire block in case of corruption
		rtype = tCorrupt
		_, err = l.r.Seek(int64(l.bleft), 1)
		if err == nil {
			l.bleft = kBlockSize
		}
	}

	ret = make([]byte, recLen)
	_, err = io.ReadFull(l.r, ret)
	if err != nil {
		ret = nil
		return
	}
	l.bleft -= recLen

	if l.checksum {
		crc := leveldb.NewCRC32C()
		crc.Write([]byte{byte(rtype)})
		crc.Write(ret)
		if crc.Sum32() != recCrc {
			ret = nil
			rtype = tCorrupt
			return
		}
	}

	return
}
