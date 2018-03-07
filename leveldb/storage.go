package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/storage"
	"sync/atomic"
)

// IStorage collects read and write statistics.
type IStorage interface {
	// Reads returns the cumulative number of read bytes of the underlying storage.
	Reads() uint64
	// Writes returns the cumulative number of written bytes of the underlying storage.
	Writes() uint64
}

type iStorage struct {
	storage.Storage
	read  uint64
	write uint64
}

func (c *iStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	r, err := c.Storage.Open(fd)
	return &meteredReader{r, c}, err
}

func (c *iStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	w, err := c.Storage.Create(fd)
	return &meteredWriter{w, c}, err
}

func (c *iStorage) Reads() uint64 {
	return atomic.LoadUint64(&c.read)
}

func (c *iStorage) Writes() uint64 {
	return atomic.LoadUint64(&c.write)
}

// AddRead increases the number of read bytes by n.
func (c *iStorage) AddRead(n uint64) uint64 {
	return atomic.AddUint64(&c.read, n)
}

// AddWrite increases the number of written bytes by n.
func (c *iStorage) AddWrite(n uint64) uint64 {
	return atomic.AddUint64(&c.write, n)
}

// iStorageWrapper returns the given storage wrapped by iStorage.
func iStorageWrapper(s storage.Storage) storage.Storage {
	return &iStorage{s, 0, 0}
}

type meteredReader struct {
	storage.Reader
	c *iStorage
}

func (r *meteredReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.c.AddRead(uint64(n))
	return n, err
}

func (r *meteredReader) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = r.Reader.ReadAt(p, off)
	r.c.AddRead(uint64(n))
	return n, err
}

type meteredWriter struct {
	storage.Writer
	c *iStorage
}

func (w *meteredWriter) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	w.c.AddWrite(uint64(n))
	return n, err
}
