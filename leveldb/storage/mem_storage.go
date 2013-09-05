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
)

type memStorageLock struct {
	stor *MemStorage
}

func (lock *memStorageLock) Release() error {
	stor := lock.stor
	stor.mu.Lock()
	defer stor.mu.Unlock()
	if stor.slock == nil {
		return ErrNotLocked
	}
	if stor.slock != lock {
		return ErrInvalidLock
	}
	stor.slock = nil
	return nil
}

// MemStorage provide implementation of memory backed storage.
type MemStorage struct {
	mu       sync.Mutex
	slock    *memStorageLock
	files    map[uint64]*memFile
	manifest *memFilePtr
}

func (m *MemStorage) init() {
	if m.files == nil {
		m.files = make(map[uint64]*memFile)
	}
}

// Lock lock the storage.
func (m *MemStorage) Lock() (Locker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.slock != nil {
		return nil, ErrLocked
	}
	m.slock = &memStorageLock{stor: m}
	return m.slock, nil
}

// Print will do nothing.
func (*MemStorage) Print(str string) {}

// GetFile get file with given number and type.
func (m *MemStorage) GetFile(num uint64, t FileType) File {
	return &memFilePtr{m: m, num: num, t: t}
}

// GetFiles get all files that match given file types; multiple file
// type may OR'ed together.
func (m *MemStorage) GetFiles(t FileType) (r []File) {
	m.mu.Lock()
	m.init()
	for num, f := range m.files {
		if f.t&t == 0 {
			continue
		}
		r = append(r, &memFilePtr{m: m, num: num, t: f.t})
	}
	m.mu.Unlock()
	return r
}

// GetManifest get manifest file.
func (m *MemStorage) GetManifest() (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manifest == nil {
		return nil, os.ErrNotExist
	}
	return m.manifest, nil
}

// SetManifest set manifest to given file.
func (m *MemStorage) SetManifest(f File) error {
	p, ok := f.(*memFilePtr)
	if !ok {
		return ErrInvalidFile
	}
	m.mu.Lock()
	m.manifest = p
	m.mu.Unlock()
	return nil
}

type memReader struct {
	bytes.Reader
}

func (*memReader) Close() error { return nil }

type memFile struct {
	bytes.Buffer
	t FileType
}

func (*memFile) Sync() error  { return nil }
func (*memFile) Close() error { return nil }

type memFilePtr struct {
	m   *MemStorage
	num uint64
	t   FileType
}

func (p *memFilePtr) Open() (Reader, error) {
	m := p.m
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	file, exist := m.files[p.num]
	if !exist || file.t != p.t {
		return nil, os.ErrNotExist
	}
	return &memReader{Reader: *bytes.NewReader(file.Bytes())}, nil
}

func (p *memFilePtr) Create() (Writer, error) {
	m := p.m
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	file := &memFile{t: p.t}
	m.files[p.num] = file
	return file, nil
}

func (p *memFilePtr) Rename(num uint64, t FileType) error {
	m := p.m
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	if file, exist := m.files[p.num]; exist && file.t == p.t {
		delete(m.files, p.num)
		file.t = t
		m.files[num] = file
		p.num = num
		p.t = t
		return nil
	}
	return os.ErrNotExist
}

func (p *memFilePtr) Exist() bool {
	m := p.m
	m.mu.Lock()
	m.init()
	file, exist := m.files[p.num]
	m.mu.Unlock()
	return exist && file.t == p.t
}

func (p *memFilePtr) Type() FileType {
	return p.t
}

func (p *memFilePtr) Num() uint64 {
	return p.num
}

func (p *memFilePtr) Size() (uint64, error) {
	m := p.m
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	if file, exist := m.files[p.num]; exist {
		return uint64(file.Len()), nil
	}
	return 0, os.ErrNotExist
}

func (p *memFilePtr) Remove() error {
	m := p.m
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	if file, exist := m.files[p.num]; exist && file.t == p.t {
		delete(m.files, p.num)
		return nil
	}
	return os.ErrNotExist
}
