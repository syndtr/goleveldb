// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

var errFileOpen = errors.New("file opened concurrently")

type testingStorageLogging interface {
	Logf(format string, args ...interface{})
}

type testingStoragePrint struct{}

func (testingStoragePrint) Logf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

type testingStorageLock struct {
	stor *testingStorage
}

func (lock *testingStorageLock) Release() error {
	stor := lock.stor
	stor.mu.Lock()
	defer stor.mu.Unlock()
	if stor.slock == nil {
		return storage.ErrNotLocked
	}
	if stor.slock != lock {
		return storage.ErrInvalidLock
	}
	stor.slock = nil
	return nil
}

type testingStorage struct {
	mu sync.Mutex

	slock *testingStorageLock
	log   testingStorageLogging

	files    map[uint64]*testingFile
	manifest *testingFilePtr

	emuCh        chan struct{}
	emuDelaySync storage.FileType
	emuWriteErr  storage.FileType
	emuSyncErr   storage.FileType
	readCnt      uint64
	readCntEn    storage.FileType
	readAtCnt    uint64
	readAtCntEn  storage.FileType
}

func newTestingStorage(log testingStorageLogging) *testingStorage {
	return &testingStorage{
		log:   log,
		files: make(map[uint64]*testingFile),
		emuCh: make(chan struct{}),
	}
}

func (d *testingStorage) wake() {
	for {
		select {
		case <-d.emuCh:
		default:
			return
		}
	}
}

func (d *testingStorage) DelaySync(t storage.FileType) {
	d.mu.Lock()
	d.emuDelaySync |= t
	d.wake()
	d.mu.Unlock()
}

func (d *testingStorage) ReleaseSync(t storage.FileType) {
	d.mu.Lock()
	d.emuDelaySync &= ^t
	d.wake()
	d.mu.Unlock()
}

func (d *testingStorage) SetWriteErr(t storage.FileType) {
	d.mu.Lock()
	d.emuWriteErr = t
	d.mu.Unlock()
}

func (d *testingStorage) SetSyncErr(t storage.FileType) {
	d.mu.Lock()
	d.emuSyncErr = t
	d.mu.Unlock()
}

func (d *testingStorage) ReadCounter() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.readCnt
}

func (d *testingStorage) ResetReadCounter() {
	d.mu.Lock()
	d.readCnt = 0
	d.mu.Unlock()
}

func (d *testingStorage) SetReadCounter(t storage.FileType) {
	d.mu.Lock()
	d.readCntEn = t
	d.mu.Unlock()
}

func (d *testingStorage) countRead(t storage.FileType) {
	d.mu.Lock()
	if d.readCntEn&t != 0 {
		d.readCnt++
	}
	d.mu.Unlock()
}

func (d *testingStorage) ReadAtCounter() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.readAtCnt
}

func (d *testingStorage) ResetReadAtCounter() {
	d.mu.Lock()
	d.readAtCnt = 0
	d.mu.Unlock()
}

func (d *testingStorage) SetReadAtCounter(t storage.FileType) {
	d.mu.Lock()
	d.readAtCntEn = t
	d.mu.Unlock()
}

func (d *testingStorage) countReadAt(t storage.FileType) {
	d.mu.Lock()
	if d.readAtCntEn&t != 0 {
		d.readAtCnt++
	}
	d.mu.Unlock()
}

func (d *testingStorage) doPrint(str string, t time.Time) {
	if d.log == nil {
		return
	}

	hour, min, sec := t.Clock()
	msec := t.Nanosecond() / 1e3
	d.log.Logf("<%02d:%02d:%02d.%06d> %s\n", hour, min, sec, msec, str)
}

func (d *testingStorage) Lock() (l storage.Locker, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.slock != nil {
		return nil, storage.ErrLocked
	}
	d.slock = &testingStorageLock{stor: d}
	return d.slock, nil
}

func (d *testingStorage) print(str string) {
	d.doPrint(str, time.Now())
}

func (d *testingStorage) Print(str string) {
	t := time.Now()
	d.mu.Lock()
	d.doPrint(str, t)
	d.mu.Unlock()
}

func (d *testingStorage) GetFile(num uint64, t storage.FileType) storage.File {
	return &testingFilePtr{stor: d, num: num, t: t}
}

func (d *testingStorage) GetFiles(t storage.FileType) (r []storage.File) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, f := range d.files {
		if f.t&t == 0 {
			continue
		}
		r = append(r, &testingFilePtr{stor: d, num: f.num, t: f.t})
	}
	return
}

func (d *testingStorage) GetManifest() (f storage.File, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.manifest == nil {
		return nil, os.ErrNotExist
	}
	return d.manifest, nil
}

func (d *testingStorage) SetManifest(f storage.File) error {
	p, ok := f.(*testingFilePtr)
	if !ok {
		return storage.ErrInvalidFile
	}
	d.mu.Lock()
	d.manifest = p
	d.mu.Unlock()
	return nil
}

func (d *testingStorage) Sizes() (n int) {
	d.mu.Lock()
	for _, file := range d.files {
		n += file.buf.Len()
	}
	d.mu.Unlock()
	return
}

func (d *testingStorage) Close() {}

type testingWriter struct {
	p *testingFile
}

func (w *testingWriter) Write(b []byte) (n int, err error) {
	p := w.p
	stor := p.stor
	stor.mu.Lock()
	defer stor.mu.Unlock()
	if stor.emuWriteErr&p.t != 0 {
		return 0, errors.New("emulated write error")
	}
	return w.p.buf.Write(b)
}

func (w *testingWriter) Sync() error {
	p := w.p
	stor := p.stor
	stor.mu.Lock()
	defer stor.mu.Unlock()
	for stor.emuDelaySync&p.t != 0 {
		stor.mu.Unlock()
		stor.emuCh <- struct{}{}
		stor.mu.Lock()
	}
	if stor.emuSyncErr&p.t != 0 {
		return errors.New("emulated sync error")
	}
	return nil
}

func (w *testingWriter) Close() error {
	p := w.p
	stor := p.stor

	stor.mu.Lock()
	stor.print(fmt.Sprintf("testingStorage: closing writer, num=%d type=%s", p.num, p.t))
	p.opened = false
	stor.mu.Unlock()

	return nil
}

type testingReader struct {
	p *testingFile
	r *bytes.Reader
}

func (r *testingReader) Read(b []byte) (n int, err error) {
	r.p.stor.countRead(r.p.t)
	return r.r.Read(b)
}

func (r *testingReader) ReadAt(b []byte, off int64) (n int, err error) {
	r.p.stor.countReadAt(r.p.t)
	return r.r.ReadAt(b, off)
}

func (r *testingReader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

func (r *testingReader) Close() error {
	p := r.p
	stor := p.stor

	stor.mu.Lock()
	p.stor.print(fmt.Sprintf("testingStorage: closing reader, num=%d type=%s", p.num, p.t))
	p.opened = false
	stor.mu.Unlock()

	return nil
}

type testingFile struct {
	stor *testingStorage
	num  uint64
	t    storage.FileType

	buf    bytes.Buffer
	opened bool
}

type testingFilePtr struct {
	stor *testingStorage
	num  uint64
	t    storage.FileType
}

func (p *testingFilePtr) id() uint64 {
	return (p.num << 8) | uint64(p.t)
}

func (p *testingFilePtr) Open() (r storage.Reader, err error) {
	stor := p.stor

	stor.mu.Lock()
	defer stor.mu.Unlock()

	stor.print(fmt.Sprintf("testingStorage: open file, num=%d type=%s", p.num, p.t))

	f, exist := stor.files[p.id()]
	if !exist {
		return nil, os.ErrNotExist
	}

	if f.opened {
		return nil, errFileOpen
	}

	f.opened = true
	r = &testingReader{f, bytes.NewReader(f.buf.Bytes())}
	return
}

func (p *testingFilePtr) Create() (w storage.Writer, err error) {
	stor := p.stor

	stor.mu.Lock()
	defer stor.mu.Unlock()

	stor.print(fmt.Sprintf("testingStorage: create file, num=%d type=%s", p.num, p.t))

	f, exist := stor.files[p.id()]
	if exist {
		if f.opened {
			return nil, errFileOpen
		}
	} else {
		f = &testingFile{stor: stor, num: p.num, t: p.t}
		stor.files[p.id()] = f
	}

	f.opened = true
	f.buf.Reset()
	return &testingWriter{f}, nil
}

func (p *testingFilePtr) Rename(num uint64, t storage.FileType) error {
	stor := p.stor

	stor.mu.Lock()
	defer stor.mu.Unlock()

	stor.print(fmt.Sprintf("testingStorage: rename file, from num=%d type=%s, to num=%d type=%d", p.num, p.t, num, t))

	oid := p.id()
	p.num = num
	p.t = t

	if f, exist := stor.files[oid]; exist {
		if f.opened {
			return errFileOpen
		}
		delete(stor.files, oid)
		f.num = num
		f.t = t
		stor.files[p.id()] = f
	}

	return nil
}

func (p *testingFilePtr) Exist() bool {
	stor := p.stor

	stor.mu.Lock()
	defer stor.mu.Unlock()

	_, exist := stor.files[p.id()]
	return exist
}

func (p *testingFilePtr) Type() storage.FileType {
	stor := p.stor
	stor.mu.Lock()
	defer stor.mu.Unlock()
	return p.t
}

func (p *testingFilePtr) Num() uint64 {
	stor := p.stor
	stor.mu.Lock()
	defer stor.mu.Unlock()
	return p.num
}

func (p *testingFilePtr) Size() (size uint64, err error) {
	stor := p.stor

	stor.mu.Lock()
	defer stor.mu.Unlock()

	if f, exist := stor.files[p.id()]; exist {
		return uint64(f.buf.Len()), nil
	}

	return 0, os.ErrNotExist
}

func (p *testingFilePtr) Remove() error {
	stor := p.stor

	stor.mu.Lock()
	defer stor.mu.Unlock()

	stor.print(fmt.Sprintf("testingStorage: removing file, num=%d type=%s", p.num, p.t))

	if f, exist := stor.files[p.id()]; exist {
		if f.opened {
			return errFileOpen
		}
		f.buf.Reset()
		delete(stor.files, p.id())
	}

	return nil
}
