// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package descriptor

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

	"leveldb/errors"
)

// FileDesc provide implementation of file-system backed leveldb descriptor.
type FileDesc struct {
	path string
	lock *os.File
	log  *os.File
	buf  []byte
	mu   sync.Mutex
}

// OpenFile create new initialized FileDesc for given path. This will also
// hold file lock; thus any subsequent attempt to open same file path will
// fail.
func OpenFile(dbpath string) (d *FileDesc, err error) {
	err = os.MkdirAll(dbpath, 0755)
	if err != nil {
		return
	}

	lock, err := os.OpenFile(path.Join(dbpath, "LOCK"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			lock.Close()
		}
	}()

	err = setFileLock(lock, true)
	if err != nil {
		return
	}

	os.Rename(path.Join(dbpath, "LOG"), path.Join(dbpath, "LOG.old"))
	log, err := os.OpenFile(path.Join(dbpath, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		setFileLock(lock, false)
		return
	}

	d = &FileDesc{path: dbpath, lock: lock, log: log}
	runtime.SetFinalizer(d, (*FileDesc).Close)

	return
}

// Cheap integer to fixed-width decimal ASCII.  Give a negative width to avoid zero-padding.
// Knows the buffer has capacity.
func itoa(buf *[]byte, i int, wid int) {
	var u uint = uint(i)
	if u == 0 && wid <= 1 {
		*buf = append(*buf, '0')
		return
	}

	// Assemble decimal in reverse order.
	var b [32]byte
	bp := len(b)
	for ; u > 0 || wid > 0; u /= 10 {
		bp--
		wid--
		b[bp] = byte(u%10) + '0'
	}
	*buf = append(*buf, b[bp:]...)
}

// Print write given str to the log file.
func (d *FileDesc) Print(str string) {
	t := time.Now()
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	msec := t.Nanosecond() / 1e3
	d.mu.Lock()
	d.buf = d.buf[:0]

	// date
	itoa(&d.buf, year, 4)
	d.buf = append(d.buf, '/')
	itoa(&d.buf, int(month), 2)
	d.buf = append(d.buf, '/')
	itoa(&d.buf, day, 4)
	d.buf = append(d.buf, ' ')

	// time
	itoa(&d.buf, hour, 2)
	d.buf = append(d.buf, ':')
	itoa(&d.buf, min, 2)
	d.buf = append(d.buf, ':')
	itoa(&d.buf, sec, 2)
	d.buf = append(d.buf, '.')
	itoa(&d.buf, msec, 6)
	d.buf = append(d.buf, ' ')

	// write
	d.log.Write(d.buf)
	d.log.WriteString(str)
	d.log.WriteString("\n")

	d.mu.Unlock()
}

// GetFile get file with given number and type.
func (d *FileDesc) GetFile(number uint64, t FileType) File {
	return &file{desc: d, num: number, t: t}
}

// GetFiles get all files that match given file types; multiple file
// type may OR'ed together.
func (d *FileDesc) GetFiles(t FileType) (r []File) {
	dir, err := os.Open(d.path)
	if err != nil {
		return
	}
	names, err := dir.Readdirnames(0)
	dir.Close()
	if err != nil {
		return
	}
	p := &file{desc: d}
	for _, name := range names {
		if p.parse(name) && (p.t&t) != 0 {
			r = append(r, p)
			p = &file{desc: d}
		}
	}
	return
}

// GetMainManifest get main manifest file.
func (d *FileDesc) GetMainManifest() (f File, err error) {
	pth := path.Join(d.path, "CURRENT")
	rw, err := os.OpenFile(pth, os.O_RDONLY, 0)
	if err != nil {
		err = err.(*os.PathError).Err
		return
	}
	defer rw.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(rw)
	if err != nil {
		return
	}
	b := buf.Bytes()
	p := &file{desc: d}
	if len(b) < 1 || b[len(b)-1] != '\n' || !p.parse(string(b[:len(b)-1])) {
		return nil, errors.ErrCorrupt("invalid CURRENT file")
	}
	return p, nil
}

// SetMainManifest set main manifest to given file.
func (d *FileDesc) SetMainManifest(f File) (err error) {
	p, ok := f.(*file)
	if !ok {
		return ErrInvalidFile
	}
	pth := path.Join(d.path, "CURRENT")
	pthTmp := fmt.Sprintf("%s.%d", pth, p.num)
	rw, err := os.OpenFile(pthTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer rw.Close()
	_, err = fmt.Fprintln(rw, p.name())
	if err != nil {
		return
	}
	return os.Rename(pthTmp, pth)
}

// Close closes the descriptor and release the lock.
func (d *FileDesc) Close() error {
	if err := setFileLock(d.lock, false); err != nil {
		return err
	}
	return d.lock.Close()
}

type file struct {
	desc *FileDesc
	num  uint64
	t    FileType
}

func (p *file) Open() (r Reader, err error) {
	return os.OpenFile(p.path(), os.O_RDONLY, 0)
}

func (p *file) Create() (w Writer, err error) {
	return os.OpenFile(p.path(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func (p *file) Rename(num uint64, t FileType) error {
	oldPth := p.path()
	p.num = num
	p.t = t
	return os.Rename(oldPth, p.path())
}

func (p *file) Exist() bool {
	_, err := os.Stat(p.path())
	return err == nil
}

func (p *file) Type() FileType {
	return p.t
}

func (p *file) Num() uint64 {
	return p.num
}

func (p *file) Size() (size uint64, err error) {
	fi, err := os.Stat(p.path())
	if err == nil {
		size = uint64(fi.Size())
	}
	return
}

func (p *file) Remove() error {
	return os.Remove(p.path())
}

func (p *file) name() string {
	switch p.t {
	case TypeManifest:
		return fmt.Sprintf("MANIFEST-%d", p.num)
	case TypeJournal:
		return fmt.Sprintf("%d.log", p.num)
	case TypeTable:
		return fmt.Sprintf("%d.sst", p.num)
	default:
		panic("invalid file type")
	}
	return ""
}

func (p *file) path() string {
	return path.Join(p.desc.path, p.name())
}

func (p *file) parse(name string) bool {
	var num uint64
	var tail string
	_, err := fmt.Sscanf(name, "%d.%s", &num, &tail)
	if err == nil {
		switch tail {
		case "log":
			p.t = TypeJournal
		case "sst":
			p.t = TypeTable
		default:
			return false
		}
		p.num = num
		return true
	}
	n, _ := fmt.Sscanf(name, "MANIFEST-%d%s", &num, &tail)
	if n == 1 {
		p.t = TypeManifest
		p.num = num
		return true
	}

	return false
}
