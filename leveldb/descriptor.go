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

package leveldb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
)

type FileType uint32

const (
	TypeManifest FileType = 1 << iota
	TypeLog
	TypeTable
	TypeTemp
)

type Syncer interface {
	Sync() error
}

type Reader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

type Writer interface {
	io.Writer
	io.Closer
	Syncer
}

type File interface {
	// Open file for read.
	// Should return os.ErrNotExist if the file does not exist.
	Open() (r Reader, err error)

	// Create file write. Truncate if file already exist.
	Create() (w Writer, err error)

	// Rename to given number and type.
	Rename(number uint64, t FileType) error

	// Return true if the file is exist.
	Exist() bool

	// Return file type.
	Type() FileType

	// Return file number
	Number() uint64

	// Return size of the file.
	Size() (size uint64, err error)

	// Remove file.
	Remove() error
}

type Descriptor interface {
	// Get file with given number and type.
	GetFile(number uint64, t FileType) File

	// Get all files.
	GetFiles(t FileType) []File

	// Get main manifest.
	// Should return os.ErrNotExist if there's no main manifest.
	GetMainManifest() (f File, err error)

	// Set main manifest to 'f'.
	SetMainManifest(f File) error

	// Close descriptor
	Close()
}

type StdDescriptor struct {
	path string
	lock *os.File
}

func OpenDescriptor(dbpath string) (d *StdDescriptor, err error) {
	var lock *os.File
	lock, err = os.OpenFile(path.Join(dbpath, "LOCK"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	err = setFileLock(lock, true)
	if err != nil {
		lock.Close()
		return
	}
	return &StdDescriptor{path: dbpath, lock: lock}, nil
}

func (d *StdDescriptor) GetFile(number uint64, t FileType) File {
	return &stdFile{desc: d, num: number, t: t}
}

func (d *StdDescriptor) GetFiles(t FileType) (r []File) {
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

func (d *StdDescriptor) GetMainManifest() (f File, err error) {
	pth := path.Join(d.path, "CURRENT")
	var file *os.File
	file, err = os.OpenFile(pth, os.O_RDONLY, 0)
	if err != nil {
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
		return nil, ErrCorrupt("invalid CURRENT file")
	}
	return p, nil
}

func (d *StdDescriptor) SetMainManifest(f File) (err error) {
	p := f.(*stdFile)
	pth := path.Join(d.path, "CURRENT")
	pthTmp := fmt.Sprintf("%s.%d", pth, p.num)
	var file *os.File
	file, err = os.OpenFile(pthTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	_, err = file.WriteString(p.name() + "\n")
	if err != nil {
		return
	}
	file.Close()
	err = os.Remove(pth)
	if err != nil {
		return
	}
	return os.Rename(pthTmp, pth)

}

func (d *StdDescriptor) Close() {
	setFileLock(d.lock, false)
	d.lock.Close()
}

type stdFile struct {
	desc *StdDescriptor
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
	var fi os.FileInfo
	fi, err = os.Stat(p.path())
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
