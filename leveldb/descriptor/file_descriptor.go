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

package descriptor

import (
	"bytes"
	"fmt"
	"leveldb/errors"
	"os"
	"path"
	"sync"
	"time"
)

type FileDescriptor struct {
	path string
	lock *os.File
	log  *os.File
	buf  []byte
	mu   sync.Mutex
}

func OpenFile(dbpath string) (d *FileDescriptor, err error) {
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

	return &FileDescriptor{path: dbpath, lock: lock, log: log}, nil
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

func (d *FileDescriptor) Print(str string) {
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

func (d *FileDescriptor) GetFile(number uint64, t FileType) File {
	return &stdFile{desc: d, num: number, t: t}
}

func (d *FileDescriptor) GetFiles(t FileType) (r []File) {
	dir, err := os.Open(d.path)
	if err != nil {
		return
	}
	names, err := dir.Readdirnames(0)
	dir.Close()
	if err != nil {
		return
	}
	p := &stdFile{desc: d}
	for _, name := range names {
		if p.parse(name) && (p.t&t) != 0 {
			r = append(r, p)
			p = &stdFile{desc: d}
		}
	}
	return
}

func (d *FileDescriptor) GetMainManifest() (f File, err error) {
	pth := path.Join(d.path, "CURRENT")
	file, err := os.OpenFile(pth, os.O_RDONLY, 0)
	if err != nil {
		err = err.(*os.PathError).Err
		return
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(file)
	if err != nil {
		return
	}
	b := buf.Bytes()
	p := &stdFile{desc: d}
	if len(b) < 1 || b[len(b)-1] != '\n' || !p.parse(string(b[:len(b)-1])) {
		return nil, errors.ErrCorrupt("invalid CURRENT file")
	}
	return p, nil
}

func (d *FileDescriptor) SetMainManifest(f File) (err error) {
	p, ok := f.(*stdFile)
	if !ok {
		return ErrInvalidFile
	}
	pth := path.Join(d.path, "CURRENT")
	pthTmp := fmt.Sprintf("%s.%d", pth, p.num)
	file, err := os.OpenFile(pthTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	_, err = file.WriteString(p.name() + "\n")
	if err != nil {
		return
	}
	file.Close()
	os.Remove(pth)
	return os.Rename(pthTmp, pth)

}

func (d *FileDescriptor) Close() {
	setFileLock(d.lock, false)
	d.lock.Close()
}

type stdFile struct {
	desc *FileDescriptor
	num  uint64
	t    FileType
}

func (p *stdFile) Open() (r Reader, err error) {
	return os.OpenFile(p.path(), os.O_RDONLY, 0)
}

func (p *stdFile) Create() (w Writer, err error) {
	return os.OpenFile(p.path(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func (p *stdFile) Rename(num uint64, t FileType) error {
	oldPth := p.path()
	p.num = num
	p.t = t
	return os.Rename(oldPth, p.path())
}

func (p *stdFile) Exist() bool {
	_, err := os.Stat(p.path())
	return err == nil
}

func (p *stdFile) Type() FileType {
	return p.t
}

func (p *stdFile) Number() uint64 {
	return p.num
}

func (p *stdFile) Size() (size uint64, err error) {
	fi, err := os.Stat(p.path())
	if err == nil {
		size = uint64(fi.Size())
	}
	return
}

func (p *stdFile) Remove() error {
	return os.Remove(p.path())
}

func (p *stdFile) name() string {
	switch p.t {
	case TypeManifest:
		return fmt.Sprintf("MANIFEST-%d", p.num)
	case TypeLog:
		return fmt.Sprintf("%d.log", p.num)
	case TypeTable:
		return fmt.Sprintf("%d.sst", p.num)
	case TypeTemp:
		return fmt.Sprintf("%d.dbtmp", p.num)
	default:
		panic("invalid file type")
	}
	return ""
}

func (p *stdFile) path() string {
	return path.Join(p.desc.path, p.name())
}

func (p *stdFile) parse(name string) bool {
	var num uint64
	var tail string
	_, err := fmt.Sscanf(name, "%d.%s", &num, &tail)
	if err == nil {
		switch tail {
		case "log":
			p.t = TypeLog
		case "sst":
			p.t = TypeTable
		case "dbtmp":
			p.t = TypeTemp
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
