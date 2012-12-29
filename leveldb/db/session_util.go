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
	"leveldb/descriptor"
	"leveldb/log"
)

// logging

func (s *session) print(v ...interface{}) {
	s.desc.Print(fmt.Sprint(v...))
}

func (s *session) printf(format string, v ...interface{}) {
	s.desc.Print(fmt.Sprintf(format, v...))
}

// file utils

func (s *session) getLogFile(num uint64) descriptor.File {
	return s.desc.GetFile(num, descriptor.TypeLog)
}

func (s *session) getTableFile(num uint64) descriptor.File {
	return s.desc.GetFile(num, descriptor.TypeTable)
}

func (s *session) getTempFile(num uint64) descriptor.File {
	return s.desc.GetFile(num, descriptor.TypeTemp)
}

func (s *session) getFiles(t descriptor.FileType) []descriptor.File {
	return s.desc.GetFiles(t)
}

// session state

type stateNum struct {
	num        uint64
	isCommited bool
}

func (p *stateNum) set(num uint64) {
	if p.num != num {
		p.num = num
		p.isCommited = false
	}
}

func (p *stateNum) commited(num uint64) {
	if p.num == num {
		p.isCommited = true
	}
}

func (s *session) setVersion(v *version) {
	st := &s.st
	st.Lock()
	defer st.Unlock()
	st.version = v
	st.versions = append(st.versions, v)
}

func (s *session) setFileNum(num uint64) {
	st := &s.st
	st.Lock()
	defer st.Unlock()
	st.nextNum.set(num)
}

func (s *session) markFileNum(num uint64) {
	st := &s.st
	st.Lock()
	defer st.Unlock()
	if num > st.nextNum.num {
		st.nextNum.set(num)
	}
}

func (s *session) allocFileNumNL() (num uint64) {
	st := &s.st
	num = st.nextNum.num
	st.nextNum.num++
	st.nextNum.isCommited = false
	return
}

func (s *session) allocFileNum() (num uint64) {
	s.st.Lock()
	defer s.st.Unlock()
	return s.allocFileNumNL()
}

func (s *session) reuseFileNum(num uint64) {
	st := &s.st
	st.Lock()
	if st.nextNum.num == num+1 {
		st.nextNum.num = num
	}
	st.Unlock()
}

func (s *session) version() *version {
	st := &s.st
	st.Lock()
	defer st.Unlock()
	return st.version
}

func (s *session) sequence() uint64 {
	st := &s.st
	st.Lock()
	defer st.Unlock()
	return st.sequence
}

func (s *session) fillRecord(r *sessionRecord, snapshot bool) {
	st := &s.st
	st.RLock()
	defer st.RUnlock()

	if snapshot || !st.nextNum.isCommited {
		r.setNextNum(st.nextNum.num)
	}

	if snapshot {
		if !r.hasLogNum {
			r.setLogNum(st.logNum)
		}

		if !r.hasLogNum {
			r.setSequence(st.sequence)
		}

		for level, ik := range st.compactPointers {
			r.addCompactPointer(level, ik)
		}
	}

	if snapshot {
		r.setComparer(s.cmp.Name())
	}
}

func (s *session) recordCommited(r *sessionRecord) {
	st := &s.st
	st.Lock()
	defer st.Unlock()

	if r.hasNextNum {
		st.nextNum.commited(r.nextNum)
	}

	if r.hasLogNum {
		st.logNum = r.logNum
	}

	if r.hasSequence {
		st.sequence = r.sequence
	}

	for _, p := range r.compactPointers {
		st.compactPointers[p.level] = iKey(p.key)
	}
}

func (s *session) createManifest(num uint64, r *sessionRecord, v *version) (err error) {
	file := s.desc.GetFile(num, descriptor.TypeManifest)
	s.manifestWriter, err = file.Create()
	if err != nil {
		return
	}

	s.manifest = log.NewWriter(s.manifestWriter)

	if v == nil {
		v = s.st.version
	}

	if r == nil {
		r = new(sessionRecord)
	}
	s.fillRecord(r, true)
	v.fillRecord(r)
	defer func() {
		if err == nil {
			s.recordCommited(r)
		} else {
			s.manifest = nil
			s.manifestWriter.Close()
			s.manifestWriter = nil
			file.Remove()
		}
	}()
	err = s.manifest.Append(r.encode())
	if err != nil {
		return
	}
	return s.desc.SetMainManifest(file)
}

func (s *session) flushManifest(r *sessionRecord) (err error) {
	s.fillRecord(r, false)
	err = s.manifest.Append(r.encode())
	if err != nil {
		return
	}
	s.recordCommited(r)
	return
}
