// +build js,wasm

// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"syscall/js"
	"time"
)

var _ os.FileInfo = browserFSFileInfo{}

// browserFSFileInfo is an implementation of os.FileInfo for JavaScript/Wasm.
// It's backed by BrowserFS.
type browserFSFileInfo struct {
	js.Value
	path string
}

// Name returns the base name of the file.
func (fi browserFSFileInfo) Name() string {
	return filepath.Base(fi.path)
}

// Size returns the length of the file in bytes.
func (fi browserFSFileInfo) Size() int64 {
	return int64(fi.Value.Get("size").Int())
}

// Mode returns the file mode bits.
func (fi browserFSFileInfo) Mode() os.FileMode {
	return os.FileMode(fi.Value.Get("size").Int())
}

// ModTime returns the modification time.
func (fi browserFSFileInfo) ModTime() time.Time {
	modifiedTimeString := fi.Value.Get("mtime").String()
	modifiedTime, err := time.Parse(time.RFC3339, modifiedTimeString)
	if err != nil {
		panic(fmt.Errorf("could not convert string mtime (%q) to time.Time: %s", modifiedTimeString, err.Error()))
	}
	return modifiedTime
}

// IsDir is an abbreviation for Mode().IsDir().
func (fi browserFSFileInfo) IsDir() bool {
	return fi.Value.Call("isDirectory").Bool()
}

// underlying data source (always returns nil for wasm/js).
func (fi browserFSFileInfo) Sys() interface{} {
	return nil
}

var _ osFile = &browserFSFile{}

// browserFSFile is an implementation of osFile for JavaScript/Wasm. It's backed
// by BrowserFS.
type browserFSFile struct {
	// name is the name of the file (including path)
	path string
	// fd is a file descriptor used as a reference to the file.
	fd int
	// currOffset is the current value of the offset used for reading or writing.
	currOffset int64
}

// Stat returns the FileInfo structure describing file. If there is an error,
// it will be of type *PathError.
func (f browserFSFile) Stat() (os.FileInfo, error) {
	return browserFSStat(f.path)
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes
// read and any error encountered. At end of file, Read returns 0, io.EOF.
func (f *browserFSFile) Read(b []byte) (n int, err error) {
	bytesRead, err := f.read(b, f.currOffset)
	if bytesRead == 0 {
		return 0, io.EOF
	}
	f.currOffset += int64(bytesRead)
	return bytesRead, nil
}

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always
// returns a non-nil error when n < len(b). At end of file, that error is
// io.EOF.
func (f browserFSFile) ReadAt(b []byte, off int64) (n int, err error) {
	bytesRead, err := f.read(b, off)
	if bytesRead < len(b) {
		return bytesRead, io.EOF
	}
	return bytesRead, nil
}

func (f browserFSFile) read(b []byte, off int64) (n int, err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	// JavaScript API expects a Uint8Array which we then convert into []byte.
	buffer := js.Global().Get("Uint8Array").New(len(b))
	rawBytesRead := js.Global().Get("browserFS").Call("readSync", f.fd, buffer, 0, len(b), off)
	bytesRead := rawBytesRead.Int()
	for i := 0; i < bytesRead; i++ {
		b[i] = byte(buffer.Index(i).Int())
	}
	return bytesRead, nil
}

// Write writes len(b) bytes to the File. It returns the number of bytes
// written and an error, if any. Write returns a non-nil error when n !=
// len(b).
func (f *browserFSFile) Write(b []byte) (n int, err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	// The naive approach of using `string(b)` for the data to write doesn't work,
	// regardless of the encoding used. Encoding to hex seems like the most
	// reliable way to do it.
	rawBytesWritten := js.Global().Get("browserFS").Call("writeSync", f.fd, hex.EncodeToString(b), f.currOffset, "hex")

	// Note: This a workaround for an issue that can cause data inconsistency
	// (specifically an empty MANIFEST file). Normally fsync is not required on
	// every write unless WriteOptions.Sync is set to true. However, we have
	// observed that leveldb does not call fsync after creating a new MANIFEST
	// file and deleting the old one. In BrowserFS, this means that the contents
	// are never actually written to the file. On the next boot, leveldb will
	// attempt to read from the manifest file and return an error because it is
	// empty.
	if err := f.Sync(); err != nil {
		return 0, err
	}

	bytesWritten := rawBytesWritten.Int()
	f.currOffset += int64(bytesWritten)
	if bytesWritten != len(b) {
		return bytesWritten, io.ErrShortWrite
	}
	return bytesWritten, nil
}

// Seek sets the offset for the next Read or Write on file to offset,
// interpreted according to whence: 0 means relative to the origin of the
// file, 1 means relative to the current offset, and 2 means relative to the
// end. It returns the new offset and an error, if any. The behavior of Seek
// on a file opened with O_APPEND is not specified.
func (f *browserFSFile) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case os.SEEK_SET:
		f.currOffset = offset
		return f.currOffset, nil
	case os.SEEK_CUR:
		f.currOffset += offset
		return f.currOffset, nil
	case os.SEEK_END:
		f.currOffset = -offset
		return f.currOffset, nil
	}
	return 0, fmt.Errorf("Seek: unexpected whence value: %d", whence)
}

// Sync commits the current contents of the file to stable storage. Typically,
// this means flushing the file system's in-memory copy of recently written
// data to disk.
func (f browserFSFile) Sync() (err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	js.Global().Get("browserFS").Call("fsyncSync", f.fd)
	return nil
}

// Close closes the File, rendering it unusable for I/O. On files that support
// SetDeadline, any pending I/O operations will be canceled and return
// immediately with an error.
func (f browserFSFile) Close() (err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	js.Global().Get("browserFS").Call("closeSync", f.fd)
	return nil
}

func osStat(path string) (os.FileInfo, error) {
	if isBrowserFSSupported() {
		return browserFSStat(path)
	}
	return os.Stat(path)
}

func browserFSStat(path string) (fileInfo os.FileInfo, err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	rawFileInfo := js.Global().Get("browserFS").Call("statSync", path)
	return browserFSFileInfo{Value: rawFileInfo, path: path}, nil
}

func osOpenFile(name string, flag int, perm os.FileMode) (osFile, error) {
	if isBrowserFSSupported() {
		return browserFSOpenFile(name, flag, perm)
	}
	return os.OpenFile(name, flag, perm)
}

func osOpen(path string) (osFile, error) {
	if isBrowserFSSupported() {
		return browserFSOpenFile(path, os.O_RDONLY, os.ModePerm)
	}
	return os.Open(path)
}

func browserFSOpenFile(path string, flag int, perm os.FileMode) (file osFile, err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	jsFlag, err := toJSFlag(flag)
	if err != nil {
		return nil, err
	}
	rawFD := js.Global().Get("browserFS").Call("openSync", path, jsFlag, int(perm))
	return &browserFSFile{path: path, fd: rawFD.Int()}, nil
}

func toJSFlag(flag int) (string, error) {
	// Exactly one of O_RDONLY, O_WRONLY, or O_RDWR must be specified.
	//
	// O_RDONLY int = syscall.O_RDONLY // open the file read-only.
	// O_WRONLY int = syscall.O_WRONLY // open the file write-only.
	// O_RDWR   int = syscall.O_RDWR   // open the file read-write.
	//
	// The remaining values may be or'ed in to control behavior.
	//
	// O_APPEND int = syscall.O_APPEND // append data to the file when writing.
	// O_CREATE int = syscall.O_CREAT  // create a new file if none exists.
	// O_EXCL   int = syscall.O_EXCL   // used with O_CREATE, file must not exist.
	// O_SYNC   int = syscall.O_SYNC   // open for synchronous I/O.
	// O_TRUNC  int = syscall.O_TRUNC  // truncate regular writable file when opened.
	//

	jsFlag := "r"
	if flag&os.O_APPEND != 0 {
		if flag&os.O_WRONLY != 0 {
			return "", errors.New("cannot use both O_APPEND and O_WRONLY in js/wasm")
		}
		jsFlag = "a"
	} else if flag&os.O_WRONLY != 0 {
		jsFlag = "w"
	} else if flag&os.O_RDWR != 0 {
		jsFlag = "w+"
	}

	// TODO(albrow) support other types of flags?
	return jsFlag, nil
}

func osRemove(name string) error {
	if isBrowserFSSupported() {
		return browserFSRemove(name)
	}
	return os.Remove(name)
}

func browserFSRemove(name string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	js.Global().Get("browserFS").Call("unlinkSync", name)
	return nil
}

func Readdirnames(path string, n int) ([]string, error) {
	if isBrowserFSSupported() {
		return browserFSReaddirnames(path, n)
	}
	// In Go, this requires two steps. Open the dir, then call Readdirnames.
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return dir.Readdirnames(n)
}

func browserFSReaddirnames(path string, n int) ([]string, error) {
	rawNames := js.Global().Get("browserFS").Call("readdirSync", path)
	length := rawNames.Get("length").Int()
	if n != 0 && length > n {
		// If n > 0, only return up to n names.
		length = n
	}
	names := make([]string, length)
	for i := 0; i < length; i++ {
		names[i] = rawNames.Index(i).String()
	}
	return names, nil
}

func osMkdirAll(path string, perm os.FileMode) error {
	if isBrowserFSSupported() {
		return browserFSMkdirAll(path, perm)
	}
	return os.MkdirAll(path, perm)
}

func browserFSMkdirAll(path string, perm os.FileMode) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	// Note: mkdirAll is not supported by BrowserFS so we have to manually create
	// each directory.
	names := strings.Split(path, string(os.PathSeparator))
	for i := range names {
		partialPath := filepath.Join(names[:i+1]...)
		if err := browserFSMkdir(partialPath, perm); err != nil {
			if os.IsExist(err) {
				// If the directory already exists, that's fine.
				continue
			}
		}
	}
	return nil
}

func browserFSMkdir(dir string, perm os.FileMode) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if jsErr, ok := e.(js.Error); ok {
				err = convertJSError(jsErr)
			}
		}
	}()
	js.Global().Get("browserFS").Call("mkdirSync", dir, int(perm))
	return nil
}

func rename(oldpath, newpath string) error {
	if isBrowserFSSupported() {
		return browserFSRename(oldpath, newpath)
	}
	return os.Rename(oldpath, newpath)
}

func browserFSRename(oldpath, newpath string) error {
	js.Global().Get("browserFS").Call("renameSync", oldpath, newpath)
	return nil
}

func syncDir(name string) error {
	if isBrowserFSSupported() {
		return browserFSSyncDir(name)
	}

	// In Go, this is two separate steps. Open the dir, then call Sync.
	f, err := osOpen(name)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil && !isErrInvalid(err) {
		return err
	}
	return nil
}

func browserFSSyncDir(dirname string) error {
	// browserFS doesn't support syncDir directly so we need to implement it
	// manually by walking through the directory.
	names, err := browserFSReaddirnames(dirname, 0)
	if err != nil {
		return err
	}
	for _, name := range names {
		path := filepath.Join(dirname, name)
		info, err := osStat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			if err := browserFSSyncDir(path); err != nil {
				return err
			}
		} else {
			f, err := osOpen(path)
			defer f.Close()
			if err != nil {
				return err
			}
			if err := f.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

// isBrowserFSSupported returns true if BrowserFS is supported. It does this by
// checking for the global "browserFS" object.
func isBrowserFSSupported() bool {
	return js.Global().Get("browserFS") != js.Null() && js.Global().Get("browserFS") != js.Undefined()
}

// convertJSError converts an error returned by the BrowserFS API into a Go
// error. This is important because Go expects certain types of errors to be
// returned (e.g. ENOENT when a file doesn't exist) and programs often change
// their behavior depending on the type of error.
func convertJSError(err js.Error) error {
	if err.Value == js.Undefined() || err.Value == js.Null() {
		return nil
	}
	// TODO(albrow): Convert to os.PathError when possible/appropriate.
	if code := err.Get("code"); code != js.Undefined() && code != js.Null() {
		switch code.String() {
		case "ENOENT":
			return os.ErrNotExist
		case "EISDIR":
			return syscall.EISDIR
		case "EEXIST":
			return os.ErrExist
			// TODO(albrow): Fill in more codes here.
		}
	}
	return err
}

// Note: JavaScript doesn't have an flock syscall so we have to fake it. This
// won't work if another process tries to read/write to the same file. It only
// works in the context of this process, but is safe with multiple goroutines.

// locksMu protects access to readLocks and writeLocks
var locksMu = sync.Mutex{}

// readLocks is a map of path to the number of readers.
var readLocks = map[string]uint{}

// writeLocks keeps track of files which are locked for writing.
var writeLocks = map[string]struct{}{}

type browserFSFileLock struct {
	path     string
	readOnly bool
	file     osFile
}

func (fl *browserFSFileLock) release() error {
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
	var file osFile
	if file, err = osOpenFile(path, flag, 0); err != nil && os.IsNotExist(err) {
		file, err = osOpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return
	}
	if err := setFileLock(path, readOnly); err != nil {
		return nil, err
	}
	return &browserFSFileLock{file: file, path: path}, nil
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
