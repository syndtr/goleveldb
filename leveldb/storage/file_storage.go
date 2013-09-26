// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type fileLock interface {
	release() error
}

type fileStorageLock struct {
	stor *FileStorage
}

func (lock *fileStorageLock) Release() error {
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

// FileStorage provide implementation of file-system backed storage.
type FileStorage struct {
	path  string
	flock fileLock
	slock *fileStorageLock
	log   *os.File
	buf   []byte
	mu    sync.Mutex
}

// OpenFile creates new initialized FileStorage for given path. This will also
// hold file lock; thus any subsequent attempt to open same file path will
// fail.
func OpenFile(dbpath string) (d *FileStorage, err error) {
	if err = os.MkdirAll(dbpath, 0755); err != nil {
		return nil, err
	}

	flock, err := newFileLock(filepath.Join(dbpath, "LOCK"))
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			flock.release()
		}
	}()

	rename(filepath.Join(dbpath, "LOG"), filepath.Join(dbpath, "LOG.old"))
	log, err := os.OpenFile(filepath.Join(dbpath, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}

	d = &FileStorage{path: dbpath, flock: flock, log: log}
	runtime.SetFinalizer(d, (*FileStorage).Close)
	return
}

// Lock lock the storage.
func (d *FileStorage) Lock() (Locker, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.slock != nil {
		return nil, ErrLocked
	}
	d.slock = &fileStorageLock{stor: d}
	return d.slock, nil
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
func (d *FileStorage) Print(str string) {
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
func (d *FileStorage) GetFile(number uint64, t FileType) File {
	return &file{stor: d, num: number, t: t}
}

// GetFiles get all files that match given file types; multiple file
// type may OR'ed together.
func (d *FileStorage) GetFiles(t FileType) (r []File) {
	dir, err := os.Open(d.path)
	if err != nil {
		return nil
	}
	names, err := dir.Readdirnames(0)
	dir.Close()
	if err != nil {
		return nil
	}
	p := &file{stor: d}
	for _, name := range names {
		if p.parse(name) && (p.t&t) != 0 {
			r = append(r, p)
			p = &file{stor: d}
		}
	}
	return r
}

// GetManifest get manifest file.
func (d *FileStorage) GetManifest() (f File, err error) {
	pth := filepath.Join(d.path, "CURRENT")
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
	p := &file{stor: d}
	if len(b) < 1 || b[len(b)-1] != '\n' || !p.parse(string(b[:len(b)-1])) {
		return nil, errors.New("storage: invalid CURRENT file")
	}
	return p, nil
}

// SetManifest set manifest to given file.
func (d *FileStorage) SetManifest(f File) error {
	p, ok := f.(*file)
	if !ok {
		return ErrInvalidFile
	}
	pth := filepath.Join(d.path, "CURRENT")
	pthTmp := fmt.Sprintf("%s.%d", pth, p.num)
	rw, err := os.OpenFile(pthTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintln(rw, p.name()); err != nil {
		rw.Close()
		return err
	}
	rw.Close()
	return rename(pthTmp, pth)
}

// Close closes the storage and release the lock.
func (d *FileStorage) Close() error {
	// Clear the finalizer.
	runtime.SetFinalizer(d, nil)
	d.log.Close()
	return d.flock.release()
}

type file struct {
	stor *FileStorage
	num  uint64
	t    FileType
}

func (p *file) Open() (Reader, error) {
	return os.OpenFile(p.path(), os.O_RDONLY, 0)
}

func (p *file) Create() (Writer, error) {
	return os.OpenFile(p.path(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func (p *file) Rename(num uint64, t FileType) error {
	oldPth := p.path()
	p.num = num
	p.t = t
	return rename(oldPth, p.path())
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
	if err != nil {
		return
	}
	size = uint64(fi.Size())
	return
}

func (p *file) Remove() error {
	return os.Remove(p.path())
}

func (p *file) name() string {
	switch p.t {
	case TypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", p.num)
	case TypeJournal:
		return fmt.Sprintf("%06d.log", p.num)
	case TypeTable:
		return fmt.Sprintf("%06d.sst", p.num)
	default:
		panic("invalid file type")
	}
	return ""
}

func (p *file) path() string {
	return filepath.Join(p.stor.path, p.name())
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
