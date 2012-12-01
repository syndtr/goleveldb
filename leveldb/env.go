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
	"io"
	"os"
)

type Syncer interface {
	Sync() error
}

type Reader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Stat() (fi os.FileInfo, err error)
}

type Writer interface {
	io.Writer
	io.Closer
	Syncer
}

type FileLock interface {
	// Release the lock acquired by a previous successful call to LockFile.
	// REQUIRES: lock was returned by a successful LockFile() call
	// REQUIRES: lock has not already been unlocked.
	Unlock() error
}

type Env interface {
	// Open file for read. Should return error if the file does not exist.
	OpenReader(name string) (f Reader, err error)

	// Open file for write. Create new file or truncate if the file already
	// exist.
	OpenWriter(name string) (f Writer, err error)

	// Returns true if the named file exists.
	FileExists(name string) bool

	// Get the size of named file.
	GetFileSize(name string) (size uint64, err error)

	// Rename file oldname to newname.
	RenameFile(oldname, newname string) error

	// Remove the named file.
	RemoveFile(name string) error

	// Return list of files in the specified directory.
	// The names are relative to "dir".
	ListDir(name string) (res []string, err error)

	// Create the specified directory.
	CreateDir(name string) error

	// Delete the specified directory.
	RemoveDir(name string) error

	// Lock the specified file.  Used to prevent concurrent access to
	// the same db by multiple processes.  On failure, stores NULL in
	// *lock and returns non-OK.
	//
	// On success, stores a pointer to the object that represents the
	// acquired lock in *lock and returns OK.  The caller should call
	// UnlockFile(*lock) to release the lock.  If the process exits,
	// the lock will be automatically released.
	//
	// If somebody else already holds the lock, finishes immediately
	// with a failure.  I.e., this call does not wait for existing locks
	// to go away.
	//
	// May create the named file if it does not already exist.
	LockFile(name string) (fl FileLock, err error)
}

var DefaultEnv = StdEnv{}

type StdEnv struct{}

func (StdEnv) OpenReader(name string) (f Reader, err error) {
	return os.OpenFile(name, os.O_RDONLY, 0)
}

func (StdEnv) OpenWriter(name string) (f Writer, err error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func (StdEnv) FileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (StdEnv) GetFileSize(name string) (size uint64, err error) {
	var fi os.FileInfo
	fi, err = os.Stat(name)
	if err == nil {
		size = uint64(fi.Size())
	}
	return
}

func (StdEnv) RenameFile(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (StdEnv) RemoveFile(name string) error {
	return os.Remove(name)
}

func (StdEnv) ListDir(name string) (res []string, err error) {
	var f *os.File
	f, err = os.Open(name)
	if err != nil {
		return
	}
	res, err = f.Readdirnames(0)
	f.Close()
	return
}

func (StdEnv) CreateDir(name string) error {
	return os.Mkdir(name, 0755)
}

func (StdEnv) RemoveDir(name string) error {
	return os.RemoveAll(name)
}

func (StdEnv) LockFile(name string) (fl FileLock, err error) {
	var f *os.File
	f, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}

	err = setFileLock(f, true)
	if err == nil {
		fl = &stdFileLock{f: f}
	} else {
		f.Close()
	}
	return
}

type stdFileLock struct {
	f *os.File
}

func (fl *stdFileLock) Unlock() (err error) {
	err = setFileLock(fl.f, false)
	fl.f.Close()
	return
}
