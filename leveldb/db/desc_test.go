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
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb/desc"
)

var errFileOpen = errors.New("file opened concurrently")

type testDescLogging interface {
	Logf(format string, args ...interface{})
}

type testDescPrint struct{}

func (testDescPrint) Logf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

type testDesc struct {
	sync.Mutex

	log testDescLogging

	files    map[uint64]*testFile
	manifest *testFilePtr

	emuCh        chan struct{}
	emuDelaySync desc.FileType
	emuWriteErr  desc.FileType
	emuSyncErr   desc.FileType
	readCnt      uint64
	readCntEn    desc.FileType
	readAtCnt    uint64
	readAtCntEn  desc.FileType
}

func newTestDesc(log testDescLogging) *testDesc {
	return &testDesc{
		log:   log,
		files: make(map[uint64]*testFile),
		emuCh: make(chan struct{}),
	}
}

func (d *testDesc) wake() {
	for {
		select {
		case <-d.emuCh:
		default:
			return
		}
	}
}

func (d *testDesc) DelaySync(t desc.FileType) {
	d.Lock()
	d.emuDelaySync |= t
	d.wake()
	d.Unlock()
}

func (d *testDesc) ReleaseSync(t desc.FileType) {
	d.Lock()
	d.emuDelaySync &= ^t
	d.wake()
	d.Unlock()
}

func (d *testDesc) SetWriteErr(t desc.FileType) {
	d.Lock()
	d.emuWriteErr = t
	d.Unlock()
}

func (d *testDesc) SetSyncErr(t desc.FileType) {
	d.Lock()
	d.emuSyncErr = t
	d.Unlock()
}

func (d *testDesc) ReadCounter() uint64 {
	d.Lock()
	defer d.Unlock()
	return d.readCnt
}

func (d *testDesc) ResetReadCounter() {
	d.Lock()
	d.readCnt = 0
	d.Unlock()
}

func (d *testDesc) SetReadCounter(t desc.FileType) {
	d.Lock()
	d.readCntEn = t
	d.Unlock()
}

func (d *testDesc) countRead(t desc.FileType) {
	d.Lock()
	if d.readCntEn&t != 0 {
		d.readCnt++
	}
	d.Unlock()
}

func (d *testDesc) ReadAtCounter() uint64 {
	d.Lock()
	defer d.Unlock()
	return d.readAtCnt
}

func (d *testDesc) ResetReadAtCounter() {
	d.Lock()
	d.readAtCnt = 0
	d.Unlock()
}

func (d *testDesc) SetReadAtCounter(t desc.FileType) {
	d.Lock()
	d.readAtCntEn = t
	d.Unlock()
}

func (d *testDesc) countReadAt(t desc.FileType) {
	d.Lock()
	if d.readAtCntEn&t != 0 {
		d.readAtCnt++
	}
	d.Unlock()
}

func (d *testDesc) doPrint(str string, t time.Time) {
	if d.log == nil {
		return
	}

	hour, min, sec := t.Clock()
	msec := t.Nanosecond() / 1e3
	d.log.Logf("<%02d:%02d:%02d.%06d> %s\n", hour, min, sec, msec, str)
}

func (d *testDesc) print(str string) {
	d.doPrint(str, time.Now())
}

func (d *testDesc) Print(str string) {
	t := time.Now()
	d.Lock()
	d.doPrint(str, t)
	d.Unlock()
}

func (d *testDesc) GetFile(num uint64, t desc.FileType) desc.File {
	return &testFilePtr{desc: d, num: num, t: t}
}

func (d *testDesc) GetFiles(t desc.FileType) (r []desc.File) {
	d.Lock()
	defer d.Unlock()
	for _, f := range d.files {
		if f.t&t == 0 {
			continue
		}
		r = append(r, &testFilePtr{desc: d, num: f.num, t: f.t})
	}
	return
}

func (d *testDesc) GetMainManifest() (f desc.File, err error) {
	d.Lock()
	defer d.Unlock()
	if d.manifest == nil {
		return nil, os.ErrNotExist
	}
	return d.manifest, nil
}

func (d *testDesc) SetMainManifest(f desc.File) error {
	p, ok := f.(*testFilePtr)
	if !ok {
		return desc.ErrInvalidFile
	}
	d.Lock()
	d.manifest = p
	d.Unlock()
	return nil
}

func (d *testDesc) Sizes() (n int) {
	d.Lock()
	for _, file := range d.files {
		n += file.buf.Len()
	}
	d.Unlock()
	return
}

func (d *testDesc) Close() {}

type testWriter struct {
	p *testFile
}

func (w *testWriter) Write(b []byte) (n int, err error) {
	p := w.p
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	if desc.emuWriteErr&p.t != 0 {
		return 0, errors.New("emulated write error")
	}
	return w.p.buf.Write(b)
}

func (w *testWriter) Sync() error {
	p := w.p
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	for desc.emuDelaySync&p.t != 0 {
		desc.Unlock()
		desc.emuCh <- struct{}{}
		desc.Lock()
	}
	if desc.emuSyncErr&p.t != 0 {
		return errors.New("emulated sync error")
	}
	return nil
}

func (w *testWriter) Close() error {
	p := w.p
	desc := p.desc

	desc.Lock()
	desc.print(fmt.Sprintf("testDesc: closing writer, num=%d type=%s", p.num, p.t))
	p.opened = false
	desc.Unlock()

	return nil
}

type testReader struct {
	p *testFile
	r *bytes.Reader
}

func (r *testReader) Read(b []byte) (n int, err error) {
	r.p.desc.countRead(r.p.t)
	return r.r.Read(b)
}

func (r *testReader) ReadAt(b []byte, off int64) (n int, err error) {
	r.p.desc.countReadAt(r.p.t)
	return r.r.ReadAt(b, off)
}

func (r *testReader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

func (r *testReader) Close() error {
	p := r.p
	desc := p.desc

	desc.Lock()
	p.desc.print(fmt.Sprintf("testDesc: closing reader, num=%d type=%s", p.num, p.t))
	p.opened = false
	desc.Unlock()

	return nil
}

type testFile struct {
	desc *testDesc
	num  uint64
	t    desc.FileType

	buf    bytes.Buffer
	opened bool
}

type testFilePtr struct {
	desc *testDesc
	num  uint64
	t    desc.FileType
}

func (p *testFilePtr) id() uint64 {
	return (p.num << 8) | uint64(p.t)
}

func (p *testFilePtr) Open() (r desc.Reader, err error) {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	desc.print(fmt.Sprintf("testDesc: open file, num=%d type=%s", p.num, p.t))

	f, exist := desc.files[p.id()]
	if !exist {
		return nil, os.ErrNotExist
	}

	if f.opened {
		return nil, errFileOpen
	}

	f.opened = true
	r = &testReader{f, bytes.NewReader(f.buf.Bytes())}
	return
}

func (p *testFilePtr) Create() (w desc.Writer, err error) {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	desc.print(fmt.Sprintf("testDesc: create file, num=%d type=%s", p.num, p.t))

	f, exist := desc.files[p.id()]
	if exist {
		if f.opened {
			return nil, errFileOpen
		}
	} else {
		f = &testFile{desc: desc, num: p.num, t: p.t}
		desc.files[p.id()] = f
	}

	f.opened = true
	f.buf.Reset()
	return &testWriter{f}, nil
}

func (p *testFilePtr) Rename(num uint64, t desc.FileType) error {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	desc.print(fmt.Sprintf("testDesc: rename file, from num=%d type=%s, to num=%d type=%d", p.num, p.t, num, t))

	oid := p.id()
	p.num = num
	p.t = t

	if f, exist := desc.files[oid]; exist {
		if f.opened {
			return errFileOpen
		}
		delete(desc.files, oid)
		f.num = num
		f.t = t
		desc.files[p.id()] = f
	}

	return nil
}

func (p *testFilePtr) Exist() bool {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	_, exist := desc.files[p.id()]
	return exist
}

func (p *testFilePtr) Type() desc.FileType {
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	return p.t
}

func (p *testFilePtr) Num() uint64 {
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	return p.num
}

func (p *testFilePtr) Size() (size uint64, err error) {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	if f, exist := desc.files[p.id()]; exist {
		return uint64(f.buf.Len()), nil
	}

	return 0, os.ErrNotExist
}

func (p *testFilePtr) Remove() error {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	desc.print(fmt.Sprintf("testDesc: removing file, num=%d type=%s", p.num, p.t))

	if f, exist := desc.files[p.id()]; exist {
		if f.opened {
			return errFileOpen
		}
		f.buf.Reset()
		delete(desc.files, p.id())
	}

	return nil
}
