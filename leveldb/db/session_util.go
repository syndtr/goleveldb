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
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/desc"
	"github.com/syndtr/goleveldb/leveldb/log"
)

// logging

func (s *session) print(v ...interface{}) {
	s.desc.Print(fmt.Sprint(v...))
}

func (s *session) printf(format string, v ...interface{}) {
	s.desc.Print(fmt.Sprintf(format, v...))
}

func (s *session) logDropFunc(tag string, num uint64) log.DropFunc {
	return func(n int, reason string) {
		s.printf("%s[%d] dropping %d bytes: %s", tag, num, n, reason)
	}
}

// file utils

func (s *session) getLogFile(num uint64) desc.File {
	return s.desc.GetFile(num, desc.TypeLog)
}

func (s *session) getTableFile(num uint64) desc.File {
	return s.desc.GetFile(num, desc.TypeTable)
}

func (s *session) getTempFile(num uint64) desc.File {
	return s.desc.GetFile(num, desc.TypeTemp)
}

func (s *session) getFiles(t desc.FileType) []desc.File {
	return s.desc.GetFiles(t)
}

// session state

// Get current version.
func (s *session) version() *version {
	return (*version)(atomic.LoadPointer(&s.stVersion))
}

// Get current version; no barrier.
func (s *session) version_NB() *version {
	return (*version)(s.stVersion)
}

// Set current version to v.
func (s *session) setVersion(v *version) {
	for {
		old := s.stVersion
		if atomic.CompareAndSwapPointer(&s.stVersion, old, unsafe.Pointer(v)) {
			if old == nil {
				v.setfin()
			} else {
				(*version)(old).next = v
			}
			break
		}
	}
}

// Get current unused file number.
func (s *session) fileNum() uint64 {
	return atomic.LoadUint64(&s.stFileNum)
}

// Get current unused file number to num.
func (s *session) setFileNum(num uint64) {
	atomic.StoreUint64(&s.stFileNum, num)
}

// Mark file number as used.
func (s *session) markFileNum(num uint64) {
	num += 1
	for {
		old, x := s.stFileNum, num
		if old > x {
			x = old
		}
		if atomic.CompareAndSwapUint64(&s.stFileNum, old, x) {
			break
		}
	}
}

// Allocate a file number.
func (s *session) allocFileNum() (num uint64) {
	return atomic.AddUint64(&s.stFileNum, 1) - 1
}

// Reuse given file number.
func (s *session) reuseFileNum(num uint64) {
	for {
		old, x := s.stFileNum, num
		if old != x+1 {
			x = old
		}
		if atomic.CompareAndSwapUint64(&s.stFileNum, old, x) {
			break
		}
	}
}

// manifest related utils

// Fill given session record obj with current states; need external
// synchronization.
func (s *session) fillRecord(r *sessionRecord, snapshot bool) {
	r.setNextNum(s.fileNum())

	if snapshot {
		if !r.hasLogNum {
			r.setLogNum(s.stLogNum)
		}

		if !r.hasLogNum {
			r.setSeq(s.stSeq)
		}

		for level, ik := range s.stCPtrs {
			r.addCompactPointer(level, ik)
		}

		r.setComparer(s.cmp.cmp.Name())
	}
}

// Mark if record has been commited, this will update session state;
// need external synchronization.
func (s *session) recordCommited(r *sessionRecord) {
	if r.hasLogNum {
		s.stLogNum = r.logNum
	}

	if r.hasSeq {
		s.stSeq = r.seq
	}

	for _, p := range r.compactPointers {
		s.stCPtrs[p.level] = iKey(p.key)
	}
}

// Create a new manifest file; need external synchronization.
func (s *session) createManifest(num uint64, r *sessionRecord, v *version) (err error) {
	w, err := newLogWriter(s.desc.GetFile(num, desc.TypeManifest))
	if err != nil {
		return
	}

	if v == nil {
		v = s.version_NB()
	}

	if r == nil {
		r = new(sessionRecord)
	}
	s.fillRecord(r, true)
	v.fillRecord(r)

	defer func() {
		if err == nil {
			s.recordCommited(r)
			if s.manifest != nil {
				s.manifest.remove()
			}
			s.manifest = w
		} else {
			w.remove()
		}
	}()

	err = w.log.Append(r.encode())
	if err != nil {
		return
	}

	err = w.writer.Sync()
	if err != nil {
		return
	}

	return s.desc.SetMainManifest(w.file)
}

// Flush record to disk.
func (s *session) flushManifest(r *sessionRecord) (err error) {
	s.fillRecord(r, false)
	err = s.manifest.log.Append(r.encode())
	if err != nil {
		return
	}
	err = s.manifest.writer.Sync()
	if err != nil {
		return
	}
	s.recordCommited(r)
	return
}
