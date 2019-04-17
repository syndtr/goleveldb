// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// +build js,wasm

package storage

import (
	"os"
	"sync"
	"syscall"
)

// Note: JavaScript doesn't have an flock syscall so we have to fake it. This
// won't work if another process tries to read/write to the same file. It only
// works in the context of this process, but is safe with multiple goroutines.

// locksMu protects access to readLocks and writeLocks
var locksMu = sync.Mutex{}

// readLocks is a map of path to the number of readers.
var readLocks = map[string]uint{}

// writeLocks keeps track
var writeLocks = map[string]struct{}{}

type jsFileLock struct {
	path     string
	readOnly bool
	file     *os.File
}

func (fl *jsFileLock) release() error {
	if fl.readOnly {
		locksMu.Lock()
		defer locksMu.Unlock()
		count, found := readLocks[fl.path]
		if found {
			if count == 1 {
				// If this is the last reader, delete the entry from the map.
				delete(readLocks, fl.path)
			} else {
				// Otherwise decrement the number of readers.
				readLocks[fl.path] = count - 1
			}
		}
	} else {
		delete(writeLocks, fl.path)
	}
	return fl.file.Close()
}

func newFileLock(path string, readOnly bool) (fl fileLock, err error) {
	var flag int
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR
	}
	var file *os.File
	if file, err = os.OpenFile(path, flag, 0); err != nil && os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return
	}
	if err := setFileLock(path, readOnly); err != nil {
		return nil, err
	}
	return &jsFileLock{file: file, path: path}, nil
}

func setFileLock(path string, readOnly bool) error {
	locksMu.Lock()
	defer locksMu.Unlock()

	// If the file is already write locked, neither writers or readers are
	// allowed.
	_, hasWriter := writeLocks[path]
	if hasWriter {
		return syscall.EAGAIN
	}

	readCount, hasReader := readLocks[path]
	if readOnly {
		// Multiple concurrent readers are allowed. Increment the read counter.
		if hasReader {
			readLocks[path] = readCount + 1
		} else {
			readLocks[path] = 1
		}
	} else {
		// Writers are not allowed if the file is read locked.
		if hasReader {
			return syscall.EAGAIN
		}
		writeLocks[path] = struct{}{}
	}

	return nil
}

func rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func isErrInvalid(err error) bool {
	if err == os.ErrInvalid {
		return true
	}
	// Go < 1.8
	if syserr, ok := err.(*os.SyscallError); ok && syserr.Err == syscall.EINVAL {
		return true
	}
	// Go >= 1.8 returns *os.PathError instead
	if patherr, ok := err.(*os.PathError); ok && patherr.Err == syscall.EINVAL {
		return true
	}
	return false
}

func syncDir(name string) error {
	// As per fsync manpage, Linux seems to expect fsync on directory, however
	// some system don't support this, so we will ignore syscall.EINVAL.
	//
	// From fsync(2):
	//   Calling fsync() does not necessarily ensure that the entry in the
	//   directory containing the file has also reached disk. For that an
	//   explicit fsync() on a file descriptor for the directory is also needed.
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil && !isErrInvalid(err) {
		return err
	}
	return nil
}
