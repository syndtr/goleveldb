// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package storage provides storage abstraction for LevelDB.
package storage

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
)

// FileType represent a file type.
type FileType int

// File types.
const (
	TypeManifest FileType = 1 << iota
	TypeJournal
	TypeTable
	TypeTemp

	TypeAll = TypeManifest | TypeJournal | TypeTable | TypeTemp
)

func (t FileType) String() string {
	switch t {
	case TypeManifest:
		return "manifest"
	case TypeJournal:
		return "journal"
	case TypeTable:
		return "table"
	case TypeTemp:
		return "temp"
	}
	return fmt.Sprintf("<unknown:%d>", t)
}

// Common error.
var (
	ErrInvalidFile = errors.New("leveldb/storage: invalid file for argument")
	ErrLocked      = errors.New("leveldb/storage: already locked")
	ErrClosed      = errors.New("leveldb/storage: closed")
)

// ErrCorrupted is the type that wraps errors that indicate corruption of
// a file. Package storage has its own type instead of using
// errors.ErrCorrupted to prevent circular import.
type ErrCorrupted struct {
	Fd  FileDesc
	Err error
}

func (e *ErrCorrupted) Error() string {
	if !e.Fd.Zero() {
		return fmt.Sprintf("%v [file=%v]", e.Err, e.Fd)
	}
	return e.Err.Error()
}

// Syncer is the interface that wraps basic Sync method.
type Syncer interface {
	// Sync commits the current contents of the file to stable storage.
	Sync() error
}

// Reader is the interface that groups the basic Read, Seek, ReadAt and Close
// methods.
type Reader interface {
	io.ReadSeeker
	io.ReaderAt
	io.Closer
}

// Writer is the interface that groups the basic Write, Sync and Close
// methods.
type Writer interface {
	io.WriteCloser
	Syncer
}

// Locker is the interface that wraps Unlock method.
type Locker interface {
	Unlock()
}

// FileDesc is a 'file descriptor'.
type FileDesc struct {
	Type FileType
	Num  int64
}

func (fd FileDesc) String() string {
	switch fd.Type {
	case TypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case TypeJournal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case TypeTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case TypeTemp:
		return fmt.Sprintf("%06d.tmp", fd.Num)
	default:
		return fmt.Sprintf("%#x-%d", fd.Type, fd.Num)
	}
}

// Zero returns true if fd == (FileDesc{}).
func (fd FileDesc) Zero() bool {
	return fd == (FileDesc{})
}

// FileDescOk returns true if fd is a valid 'file descriptor'.
func FileDescOk(fd FileDesc) bool {
	switch fd.Type {
	case TypeManifest:
	case TypeJournal:
	case TypeTable:
	case TypeTemp:
	default:
		return false
	}
	return fd.Num >= 0
}

// Storage is the storage. A storage instance must be safe for concurrent use.
type Storage interface {
	// Lock locks the storage. Any subsequent attempt to call Lock will fail
	// until the last lock released.
	// Caller should call Unlock method after use.
	Lock() (Locker, error)

	// Log logs a string. This is used for logging.
	// An implementation may write to a file, stdout or simply do nothing.
	Log(str string)

	// SetMeta store 'file descriptor' that can later be acquired using GetMeta
	// method. The 'file descriptor' should point to a valid file.
	// SetMeta should be implemented in such way that changes should happen
	// atomically.
	SetMeta(fd FileDesc) error

	// GetMeta returns 'file descriptor' stored in meta. The 'file descriptor'
	// can be updated using SetMeta method.
	// Returns os.ErrNotExist if meta doesn't store any 'file descriptor', or
	// 'file descriptor' point to nonexistent file.
	GetMeta() (FileDesc, error)

	// List returns file descriptors that match the given file types.
	// The file types may be OR'ed together.
	List(ft FileType) ([]FileDesc, error)

	// Open opens file with the given 'file descriptor' read-only.
	// Returns os.ErrNotExist error if the file does not exist.
	// Returns ErrClosed if the underlying storage is closed.
	Open(fd FileDesc) (Reader, error)

	// Create creates file with the given 'file descriptor', truncate if already
	// exist and opens write-only.
	// Returns ErrClosed if the underlying storage is closed.
	Create(fd FileDesc) (Writer, error)

	// Remove removes file with the given 'file descriptor'.
	// Returns ErrClosed if the underlying storage is closed.
	Remove(fd FileDesc) error

	// Rename renames file from oldfd to newfd.
	// Returns ErrClosed if the underlying storage is closed.
	Rename(oldfd, newfd FileDesc) error

	// Close closes the storage.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the storage has been closed.
	Close() error
}

// MeteredStorage collects read and write statistics.
type MeteredStorage interface {
	// Reads returns the cumulative number of read bytes of the underlying storage.
	Reads() uint64
	// Writes returns the cumulative number of written bytes of the underlying storage.
	Writes() uint64
}

type meteredStorage struct {
	s     Storage
	read  uint64
	write uint64
}

func (ms *meteredStorage) Lock() (Locker, error) {
	return ms.s.Lock()
}

func (ms *meteredStorage) Log(str string) {
	ms.s.Log(str)
}

func (ms *meteredStorage) SetMeta(fd FileDesc) error {
	return ms.s.SetMeta(fd)
}

func (ms *meteredStorage) GetMeta() (FileDesc, error) {
	return ms.s.GetMeta()
}

func (ms *meteredStorage) List(ft FileType) ([]FileDesc, error) {
	return ms.s.List(ft)
}

func (ms *meteredStorage) Open(fd FileDesc) (Reader, error) {
	r, err := ms.s.Open(fd)
	return &meteredReader{r: r, ms: ms}, err
}

func (ms *meteredStorage) Create(fd FileDesc) (Writer, error) {
	w, err := ms.s.Create(fd)
	return &meteredWriter{w: w, ms: ms}, err
}

func (ms *meteredStorage) Remove(fd FileDesc) error {
	return ms.s.Remove(fd)
}

func (ms *meteredStorage) Rename(oldfd, newfd FileDesc) error {
	return ms.s.Rename(oldfd, newfd)
}

func (ms *meteredStorage) Close() error {
	return ms.s.Close()
}

func (ms *meteredStorage) Reads() uint64 {
	return atomic.LoadUint64(&ms.read)
}

func (ms *meteredStorage) Writes() uint64 {
	return atomic.LoadUint64(&ms.write)
}

// AddRead increases the number of read bytes by n.
func (ms *meteredStorage) AddRead(n uint64) uint64 {
	return atomic.AddUint64(&ms.read, n)
}

// AddWrite increases the number of written bytes by n.
func (ms *meteredStorage) AddWrite(n uint64) uint64 {
	return atomic.AddUint64(&ms.write, n)
}

type meteredReader struct {
	r  Reader
	ms *meteredStorage
}

func (r *meteredReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.ms.AddRead(uint64(n))
	return n, err
}

func (r *meteredReader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

func (r *meteredReader) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = r.r.ReadAt(p, off)
	r.ms.AddRead(uint64(n))
	return n, err
}

func (r *meteredReader) Close() error {
	return r.r.Close()
}

type meteredWriter struct {
	w  Writer
	ms *meteredStorage
}

func (w *meteredWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.ms.AddWrite(uint64(n))
	return n, err
}

func (w *meteredWriter) Close() error {
	return w.w.Close()
}

func (w *meteredWriter) Sync() error {
	return w.w.Sync()
}
