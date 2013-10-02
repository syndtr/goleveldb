// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"bytes"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/util"
)

type memStorageLock struct {
	ms *memStorage
}

func (lock *memStorageLock) Release() {
	ms := lock.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock == lock {
		ms.slock = nil
	}
	return
}

// memStorage is a memory-backed storage.
type memStorage struct {
	mu       sync.Mutex
	slock    *memStorageLock
	files    map[uint64]*memFile
	manifest *memFilePtr
}

// NewMemStorage returns a new memory-backed storage implementation.
func NewMemStorage() Storage {
	return &memStorage{
		files: make(map[uint64]*memFile),
	}
}

func (ms *memStorage) Lock() (util.Releaser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock != nil {
		return nil, ErrLocked
	}
	ms.slock = &memStorageLock{ms: ms}
	return ms.slock, nil
}

func (*memStorage) Log(str string) {}

func (ms *memStorage) GetFile(num uint64, t FileType) File {
	return &memFilePtr{ms: ms, num: num, t: t}
}

func (ms *memStorage) GetFiles(t FileType) ([]File, error) {
	ms.mu.Lock()
	var ff []File
	for num, f := range ms.files {
		if f.t&t == 0 {
			continue
		}
		ff = append(ff, &memFilePtr{ms: ms, num: num, t: f.t})
	}
	ms.mu.Unlock()
	return ff, nil
}

func (ms *memStorage) GetManifest() (File, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.manifest == nil {
		return nil, os.ErrNotExist
	}
	return ms.manifest, nil
}

func (ms *memStorage) SetManifest(f File) error {
	fm, ok := f.(*memFilePtr)
	if !ok || fm.t != TypeManifest {
		return ErrInvalidFile
	}
	ms.mu.Lock()
	ms.manifest = fm
	ms.mu.Unlock()
	return nil
}

func (*memStorage) Close() error { return nil }

type memReader struct {
	*bytes.Reader
	m *memFile
}

func (mr *memReader) Close() error {
	return mr.m.Close()
}

type memFile struct {
	bytes.Buffer
	ms   *memStorage
	t    FileType
	open bool
}

func (*memFile) Sync() error { return nil }
func (m *memFile) Close() error {
	m.ms.mu.Lock()
	m.open = false
	m.ms.mu.Unlock()
	return nil
}

type memFilePtr struct {
	ms  *memStorage
	num uint64
	t   FileType
}

func (p *memFilePtr) Open() (Reader, error) {
	ms := p.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if m, exist := ms.files[p.num]; exist && m.t == p.t {
		if m.open {
			return nil, errFileOpen
		}
		m.open = true
		return &memReader{Reader: bytes.NewReader(m.Bytes()), m: m}, nil
	}
	return nil, os.ErrNotExist
}

func (p *memFilePtr) Create() (Writer, error) {
	ms := p.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	m, exist := ms.files[p.num]
	if exist && m.t == p.t {
		if m.open {
			return nil, errFileOpen
		}
		m.Reset()
	} else {
		m = &memFile{ms: ms, t: p.t}
		ms.files[p.num] = m
	}
	m.open = true
	return m, nil
}

func (p *memFilePtr) Type() FileType {
	return p.t
}

func (p *memFilePtr) Num() uint64 {
	return p.num
}

func (p *memFilePtr) Remove() error {
	ms := p.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if m, exist := ms.files[p.num]; exist && m.t == p.t {
		delete(ms.files, p.num)
		return nil
	}
	return os.ErrNotExist
}
