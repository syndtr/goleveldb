// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENE file.

package leveldb

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	tsErrInvalidFile = errors.New("leveldb.testStorage: invalid file for argument")
	tsErrLocked      = errors.New("leveldb.testStorage: already locked")
	tsErrFileOpen    = errors.New("leveldb.testStorage: file still open")
	tsErrClosed      = errors.New("leveldb.testStorage: storage closed")
)

type testStorageLock struct {
	ts *testStorage
}

func (lock *testStorageLock) Release() {
	ts := lock.ts
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.slock == lock {
		ts.slock = nil
		ts.t.Log("I: storage unlocked")
	} else {
		ts.t.Error("E: storage unlocking failed: invalid lock handle")
	}
	return
}

// testStorage is a memory-backed storage used for testing.
type testStorage struct {
	t *testing.T

	mu           sync.Mutex
	cond         sync.Cond
	slock        *testStorageLock
	files        map[uint64]*memFile
	manifest     *memFilePtr
	open         int
	emuDelaySync storage.FileType
	emuWriteErr  storage.FileType
	emuSyncErr   storage.FileType
	readCnt      uint64
	readCntEn    storage.FileType
}

func newTestStorage(t *testing.T) *testStorage {
	ts := &testStorage{
		t:     t,
		files: make(map[uint64]*memFile),
	}
	ts.cond.L = &ts.mu
	return ts
}

func (ts *testStorage) DelaySync(t storage.FileType) {
	ts.mu.Lock()
	ts.emuDelaySync |= t
	ts.cond.Broadcast()
	ts.mu.Unlock()
}

func (ts *testStorage) ReleaseSync(t storage.FileType) {
	ts.mu.Lock()
	ts.emuDelaySync &= ^t
	ts.cond.Broadcast()
	ts.mu.Unlock()
}

func (ts *testStorage) SetWriteErr(t storage.FileType) {
	ts.mu.Lock()
	ts.emuWriteErr = t
	ts.mu.Unlock()
}

func (ts *testStorage) SetSyncErr(t storage.FileType) {
	ts.mu.Lock()
	ts.emuSyncErr = t
	ts.mu.Unlock()
}

func (ts *testStorage) ReadCounter() uint64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.readCnt
}

func (ts *testStorage) ResetReadCounter() {
	ts.mu.Lock()
	ts.readCnt = 0
	ts.mu.Unlock()
}

func (ts *testStorage) SetReadCounter(t storage.FileType) {
	ts.mu.Lock()
	ts.readCntEn = t
	ts.mu.Unlock()
}

func (ts *testStorage) countRead(t storage.FileType) {
	ts.mu.Lock()
	if ts.readCntEn&t != 0 {
		ts.readCnt++
	}
	ts.mu.Unlock()
}

func (ts *testStorage) Sizes() (n int) {
	ts.mu.Lock()
	for _, m := range ts.files {
		n += m.Len()
	}
	ts.mu.Unlock()
	return
}

// Lock lock the storage.
func (ts *testStorage) Lock() (util.Releaser, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.slock != nil {
		ts.t.Log("W: storage locking failed: already locked")
		return nil, tsErrLocked
	}
	ts.t.Log("I: storage locked")
	ts.slock = &testStorageLock{ts: ts}
	return ts.slock, nil
}

func (ts *testStorage) Log(str string) {
	ts.t.Log("P: " + str)
}

func (ts *testStorage) GetFile(num uint64, t storage.FileType) storage.File {
	return &memFilePtr{ts: ts, num: num, t: t}
}

func (ts *testStorage) GetFiles(t storage.FileType) ([]storage.File, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.open < 0 {
		ts.t.Error("E: get files failed: closed")
		return nil, tsErrClosed
	}
	var ff []storage.File
	for num, f := range ts.files {
		if f.t&t == 0 {
			continue
		}
		ff = append(ff, &memFilePtr{ts: ts, num: num, t: f.t})
	}
	ts.t.Logf("I: get files, type=0x%x count=%d", t, len(ff))
	return ff, nil
}

func (ts *testStorage) GetManifest() (storage.File, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.manifest == nil {
		ts.t.Log("W: get manifest failed: not found")
		return nil, os.ErrNotExist
	}
	ts.t.Logf("I: get manifest, num=%d", ts.manifest.num)
	return ts.manifest, nil
}

func (ts *testStorage) SetManifest(f storage.File) error {
	fm, ok := f.(*memFilePtr)
	if !ok {
		ts.t.Error("I: set manifest failed: type assertion failed")
		return tsErrInvalidFile
	} else if fm.t != storage.TypeManifest {
		ts.t.Error("I: set manifest failed: invalid file type")
		return tsErrInvalidFile
	}
	ts.t.Logf("I: set manifest, num=%d", fm.num)
	ts.mu.Lock()
	ts.manifest = fm
	ts.mu.Unlock()
	return nil
}

func (ts *testStorage) Close() error {
	return nil
}

func (ts *testStorage) CheckClosed() {
	ts.mu.Lock()
	if ts.open == 0 {
		ts.t.Log("I: all files are closed")
	} else if ts.open > 0 {
		ts.t.Errorf("E: %d files still open", ts.open)
		for num, m := range ts.files {
			if m.open {
				ts.t.Errorf("E:  num=%d type=%v", num, m.t)
			}
		}
	}
	ts.mu.Unlock()
}

type memReader struct {
	*bytes.Reader
	m *memFile
}

func (mr *memReader) Read(b []byte) (n int, err error) {
	m := mr.m
	m.ts.countRead(m.t)
	return mr.Reader.Read(b)
}

func (mr *memReader) Close() error {
	return mr.m.close("reader")
}

type memFile struct {
	bytes.Buffer
	ts   *testStorage
	num  uint64
	t    storage.FileType
	open bool
}

func (m *memFile) Write(b []byte) (n int, err error) {
	ts := m.ts
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.emuWriteErr&m.t != 0 {
		return 0, errors.New("leveldb.testStorage: emulated write error")
	}
	return m.Buffer.Write(b)
}

func (m *memFile) Sync() error {
	ts := m.ts
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for ts.emuDelaySync&m.t != 0 {
		ts.cond.Wait()
	}
	if ts.emuSyncErr&m.t != 0 {
		return errors.New("leveldb.testStorage: emulated sync error")
	}
	return nil
}

func (m *memFile) close(kind string) error {
	ts := m.ts
	ts.mu.Lock()
	if m.open {
		ts.t.Logf("I: file closed, kind=%d num=%d type=%v", kind, m.num, m.t)
		m.open = false
	} else {
		ts.t.Errorf("E: redudant file closing, kind=%s num=%d type=%v", kind, m.num, m.t)
	}
	ts.mu.Unlock()
	return nil
}

func (m *memFile) Close() error {
	return m.close("writer")
}

type memFilePtr struct {
	ts  *testStorage
	num uint64
	t   storage.FileType
}

func (p *memFilePtr) Open() (storage.Reader, error) {
	ts := p.ts
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if m, exist := ts.files[p.num]; exist && m.t == p.t {
		if m.open {
			m.ts.t.Errorf("E: file open failed, num=%d type=%v: already open", p.num, p.t)
			return nil, tsErrFileOpen
		}
		m.ts.t.Logf("I: file opened, num=%d type=%v", p.num, p.t)
		m.open = true
		return &memReader{Reader: bytes.NewReader(m.Bytes()), m: m}, nil
	}
	ts.t.Errorf("E: file open failed, num=%d type=%v: doesn't exist", p.num, p.t)
	return nil, os.ErrNotExist
}

func (p *memFilePtr) Create() (storage.Writer, error) {
	ts := p.ts
	ts.mu.Lock()
	defer ts.mu.Unlock()
	m, exist := ts.files[p.num]
	if exist && m.t == p.t {
		if m.open {
			m.ts.t.Errorf("E: file creation failed, num=%d type=%v: file still open", p.num, p.t)
			return nil, tsErrFileOpen
		}
		m.Reset()
		ts.t.Logf("I: file created, num=%d type=%v: overwrite", p.num, p.t)
	} else {
		m = &memFile{ts: ts, num: p.num, t: p.t}
		ts.files[p.num] = m
		ts.t.Logf("I: file created, num=%d type=%v: new", p.num, p.t)
	}
	m.open = true
	return m, nil
}

func (p *memFilePtr) Type() storage.FileType {
	return p.t
}

func (p *memFilePtr) Num() uint64 {
	return p.num
}

func (p *memFilePtr) Remove() error {
	ts := p.ts
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if m, exist := ts.files[p.num]; exist && m.t == p.t {
		delete(ts.files, p.num)
		m.ts.t.Logf("I: file removed, num=%d type=%v", p.num, p.t)
		return nil
	}
	ts.t.Errorf("E: file removal failed, num=%d type=%v: doesn't exist", p.num, p.t)
	return os.ErrNotExist
}
