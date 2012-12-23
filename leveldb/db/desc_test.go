package db

import (
	"bytes"
	"errors"
	"fmt"
	"leveldb/descriptor"
	"os"
	"sync"
	"time"
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
	manifest *testFile

	emuCh        chan struct{}
	emuDelaySync descriptor.FileType
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

func (d *testDesc) DelaySync(t descriptor.FileType) {
	d.Lock()
	d.emuDelaySync |= t
	d.wake()
	d.Unlock()
}

func (d *testDesc) ReleaseSync(t descriptor.FileType) {
	d.Lock()
	d.emuDelaySync &= ^t
	d.wake()
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

func (d *testDesc) GetFile(num uint64, t descriptor.FileType) descriptor.File {
	d.Lock()
	defer d.Unlock()
	n := (num << 8) | uint64(t)
	if f, ok := d.files[n]; ok {
		return f
	}
	f := &testFile{desc: d, num: num, t: t}
	return f
}

func (d *testDesc) GetFiles(t descriptor.FileType) (r []descriptor.File) {
	d.Lock()
	defer d.Unlock()
	for _, file := range d.files {
		if file.t&t == 0 {
			continue
		}
		r = append(r, file)
	}
	return
}

func (d *testDesc) GetMainManifest() (f descriptor.File, err error) {
	d.Lock()
	defer d.Unlock()
	if d.manifest == nil {
		return nil, os.ErrNotExist
	}
	return d.manifest, nil
}

func (d *testDesc) SetMainManifest(f descriptor.File) error {
	p, ok := f.(*testFile)
	if !ok {
		return descriptor.ErrInvalidFile
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
	return p.buf.Write(b)
}

func (w *testWriter) Sync() error {
	p := w.p
	desc := p.desc
	desc.Lock()
	for desc.emuDelaySync&p.t != 0 {
		desc.Unlock()
		desc.emuCh <- struct{}{}
		desc.Lock()
	}
	desc.Unlock()
	return nil
}

func (w *testWriter) Close() error {
	p := w.p
	desc := p.desc

	desc.Lock()
	p.desc.print(fmt.Sprintf("testDesc: closing writer, num=%d type=%s", p.num, p.t))
	p.opened = false
	desc.Unlock()

	return nil
}

type testReader struct {
	p *testFile
	r *bytes.Reader
}

func (r *testReader) Read(b []byte) (n int, err error) {
	return r.r.Read(b)
}

func (r *testReader) ReadAt(b []byte, off int64) (n int, err error) {
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
	t    descriptor.FileType

	buf     bytes.Buffer
	created bool
	opened  bool
}

func (p *testFile) Open() (r descriptor.Reader, err error) {
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	if p.opened {
		return nil, errFileOpen
	}
	if !p.created {
		return nil, os.ErrNotExist
	}
	p.desc.print(fmt.Sprintf("testDesc: open file, num=%d type=%s", p.num, p.t))
	r = &testReader{p, bytes.NewReader(p.buf.Bytes())}
	return
}

func (p *testFile) Create() (w descriptor.Writer, err error) {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	if p.opened {
		return nil, errFileOpen
	}
	p.desc.print(fmt.Sprintf("testDesc: create file, num=%d type=%s", p.num, p.t))
	p.created = true
	p.opened = true
	p.buf.Reset()
	n := (p.num << 8) | uint64(p.t)
	desc.files[n] = p
	return &testWriter{p}, nil
}

func (p *testFile) Rename(num uint64, t descriptor.FileType) error {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()

	if p.opened {
		return errFileOpen
	}
	n := (p.num << 8) | uint64(p.t)
	delete(desc.files, n)
	p.num = num
	p.t = t
	n = (p.num << 8) | uint64(p.t)
	desc.files[n] = p
	return nil
}

func (p *testFile) Exist() bool {
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	return p.created
}

func (p *testFile) Type() descriptor.FileType {
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	return p.t
}

func (p *testFile) Number() uint64 {
	desc := p.desc
	desc.Lock()
	defer desc.Unlock()
	return p.num
}

func (p *testFile) Size() (size uint64, err error) {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()
	if !p.created {
		return 0, os.ErrNotExist
	}
	return uint64(p.buf.Len()), nil
}

func (p *testFile) Remove() error {
	desc := p.desc

	desc.Lock()
	defer desc.Unlock()
	desc.print(fmt.Sprintf("testDesc: removing file, num=%d type=%s", p.num, p.t))
	if p.opened {
		return errFileOpen
	}
	p.buf.Reset()
	p.created = false
	delete(desc.files, p.num)
	return nil
}
