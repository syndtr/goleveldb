// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package opt provides sets of options used by LevelDB.
package opt

import (
	"errors"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
)

var (
	ErrInvalid    = errors.New("invalid value")
	ErrNotSet     = errors.New("not set")
	ErrNotAllowed = errors.New("not allowed")
)

const (
	DefaultWriteBuffer          = 4 << 20
	DefaultMaxOpenFiles         = 1000
	DefaultBlockCacheSize       = 8 << 20
	DefaultBlockSize            = 4096
	DefaultBlockRestartInterval = 16
	DefaultCompressionType      = SnappyCompression
)

type OptionsFlag uint

const (
	// If set, the database will be created if it is missing.
	OFCreateIfMissing OptionsFlag = 1 << iota

	// If set, an error is raised if the database already exists.
	OFErrorIfExist

	// If set, the implementation will do aggressive checking of the
	// data it is processing and will stop early if it detects any
	// errors.  This may have unforeseen ramifications: for example, a
	// corruption of one DB entry may cause a large number of entries to
	// become unreadable or for the entire DB to become unopenable.
	OFParanoidCheck
)

// Database compression type
type Compression uint

func (c Compression) String() string {
	switch c {
	case DefaultCompression:
		return "default"
	case NoCompression:
		return "none"
	case SnappyCompression:
		return "snappy"
	}
	return "unknown"
}

const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	nCompression
)

// Options represent sets of LevelDB options.
type Options struct {
	// Comparer used to define the order of keys in the table.
	// Default: a comparer that uses lexicographic byte-wise
	// ordering.
	//
	// REQUIRES: The client must ensure that the comparer supplied
	// here has the same name and orders keys *exactly* the same as the
	// comparer provided to previous open calls on the same DB.
	// Additionally, the client must also make sure that the
	// supplied comparer retains the same name. Otherwise, an error
	// will be returned on reopening the database.
	Comparer comparer.Comparer

	// Specify the database flag.
	Flag OptionsFlag

	// Amount of data to build up in memory (backed by an unsorted journal
	// on disk) before converting to a sorted on-disk file.
	//
	// Larger values increase performance, especially during bulk loads.
	// Up to two write buffers may be held in memory at the same time,
	// so you may wish to adjust this parameter to control memory usage.
	// Also, a larger write buffer will result in a longer recovery time
	// the next time the database is opened.
	//
	// Default: 4MB
	WriteBuffer int

	// Number of open files that can be used by the DB.  You may need to
	// increase this if your database has a large working set (budget
	// one open file per 2MB of working set).
	//
	// Default: 1000
	MaxOpenFiles int

	// Control over blocks (user data is stored in a set of blocks, and
	// a block is the unit of reading from disk).

	// If non-NULL, use the specified cache for blocks.
	// If NULL, leveldb will automatically create and use an 8MB internal cache.
	// Default: NULL
	BlockCache cache.Cache

	// Approximate size of user data packed per block.  Note that the
	// block size specified here corresponds to uncompressed data.  The
	// actual size of the unit read from disk may be smaller if
	// compression is enabled.  This parameter can be changed dynamically.
	//
	// Default: 4K
	BlockSize int

	// Number of keys between restart points for delta encoding of keys.
	// This parameter can be changed dynamically.  Most clients should
	// leave this parameter alone.
	//
	// Default: 16
	BlockRestartInterval int

	// Compress blocks using the specified compression algorithm.  This
	// parameter can be changed dynamically.
	//
	// Default: kSnappyCompression, which gives lightweight but fast
	// compression.
	//
	// Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
	//    ~200-500MB/s compression
	//    ~400-800MB/s decompression
	// Note that these speeds are significantly faster than most
	// persistent storage speeds, and therefore it is typically never
	// worth switching to kNoCompression.  Even if the input data is
	// incompressible, the kSnappyCompression implementation will
	// efficiently detect that and will switch to uncompressed mode.
	CompressionType Compression

	// If non-NULL, use the specified filter policy to reduce disk reads.
	// Many applications will benefit from passing the result of
	// NewBloomFilter() here.
	//
	// As long as the same filter (name) was used as last time the
	// database was opened, the previous filter is reused. That is,
	// the filter does not need to be rebuilt. This is made possible
	// since each filter is persisted to disk on a per sstable
	// basis.
	//
	// As opposed to the comparer, a filter can be replaced after a
	// database has been created. If this is done, the previous
	// persisted filter will be ignored for every old sstable.
	// Every new table will use the newly introduced filter. This
	// means that all/some sstables will lack a filter during a
	// transition period. Note that this might have an impact on
	// performance. This problem can be mitigated by inserting old
	// filter into AltFilters. Also, rewriting every single key/value
	// will force introduction of the new filter.
	//
	// Default: NULL
	Filter filter.Filter

	// Define one or more alternative filters. This alternative filters
	// will be used as fallback (if respective filter present) during
	// read operation if a sstable contains filter block generated by
	// different filter than currently active filter.
	AltFilters []filter.Filter

	mu      sync.RWMutex
	filters map[string]filter.Filter
}

// OptionsGetter wraps methods used to get sanitized options.
type OptionsGetter interface {
	GetComparer() comparer.Comparer
	HasFlag(flag OptionsFlag) bool
	GetWriteBuffer() int
	GetMaxOpenFiles() int
	GetBlockCache() cache.Cache
	GetBlockSize() int
	GetBlockRestartInterval() int
	GetCompressionType() Compression
	GetFilter() filter.Filter
	GetAltFilter(name string) filter.Filter
	GetAltFilters() []filter.Filter
}

// OptionsSetter wraps methods used to set options.
type OptionsSetter interface {
	SetComparer(cmp comparer.Comparer) error
	SetFlag(flag OptionsFlag) error
	ClearFlag(flag OptionsFlag) error
	SetWriteBuffer(size int) error
	SetMaxOpenFiles(max int) error
	SetBlockCache(cache cache.Cache) error
	SetBlockCacheCapacity(capacity int) error
	SetBlockSize(size int) error
	SetBlockRestartInterval(interval int) error
	SetCompressionType(compression Compression) error
	SetFilter(p filter.Filter) error
	InsertAltFilter(p filter.Filter) error
	RemoveAltFilter(name string) error
}

// Getter

func (o *Options) GetComparer() comparer.Comparer {
	if o == nil {
		return comparer.DefaultComparer
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.Comparer == nil {
		return comparer.DefaultComparer
	}
	return o.Comparer
}

func (o *Options) HasFlag(flag OptionsFlag) bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return (o.Flag & flag) != 0
}

func (o *Options) GetWriteBuffer() int {
	if o == nil {
		return DefaultWriteBuffer
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.WriteBuffer <= 0 {
		return DefaultWriteBuffer
	}
	return o.WriteBuffer
}

func (o *Options) GetMaxOpenFiles() int {
	if o == nil {
		return DefaultMaxOpenFiles
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.MaxOpenFiles <= 0 {
		return DefaultMaxOpenFiles
	}
	return o.MaxOpenFiles
}

func (o *Options) GetBlockCache() cache.Cache {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.BlockCache
}

func (o *Options) GetBlockSize() int {
	if o == nil {
		return DefaultBlockSize
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.BlockSize <= 0 {
		return DefaultBlockSize
	}
	return o.BlockSize
}

func (o *Options) GetBlockRestartInterval() int {
	if o == nil {
		return DefaultBlockRestartInterval
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.BlockRestartInterval <= 0 {
		return DefaultBlockRestartInterval
	}
	return o.BlockRestartInterval
}

func (o *Options) GetCompressionType() Compression {
	if o == nil {
		return DefaultCompressionType
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.CompressionType <= DefaultCompression || o.CompressionType >= nCompression {
		return DefaultCompressionType
	}
	return o.CompressionType
}

func (o *Options) GetFilter() filter.Filter {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.Filter
}

func (o *Options) GetAltFilter(name string) filter.Filter {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.initFilters()
	return o.filters[name]
}

func (o *Options) GetAltFilters() []filter.Filter {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.initFilters()
	filters := make([]filter.Filter, 0, len(o.filters))
	for _, p := range o.filters {
		filters = append(filters, p)
	}
	return filters
}

// Setter

func (o *Options) SetComparer(cmp comparer.Comparer) error {
	if o == nil {
		return ErrNotSet
	}
	if cmp == nil {
		return ErrInvalid
	}
	o.mu.Lock()
	o.Comparer = cmp
	o.mu.Unlock()
	return nil
}

func (o *Options) SetFlag(flag OptionsFlag) error {
	if o == nil {
		return ErrNotSet
	}
	o.mu.Lock()
	o.Flag |= flag
	o.mu.Unlock()
	return nil
}

func (o *Options) ClearFlag(flag OptionsFlag) error {
	if o == nil {
		return ErrNotSet
	}
	o.mu.Lock()
	o.Flag &= ^flag
	o.mu.Unlock()
	return nil
}

func (o *Options) SetWriteBuffer(size int) error {
	if o == nil {
		return ErrNotSet
	}
	if size <= 0 {
		return ErrInvalid
	}
	o.mu.Lock()
	o.WriteBuffer = size
	o.mu.Unlock()
	return nil
}

func (o *Options) SetMaxOpenFiles(max int) error {
	if o == nil {
		return ErrNotSet
	}
	if max <= 0 {
		return ErrInvalid
	}
	o.mu.Lock()
	o.MaxOpenFiles = max
	o.mu.Unlock()
	return nil
}

func (o *Options) SetBlockCache(cache cache.Cache) error {
	if o == nil {
		return ErrNotSet
	}
	o.mu.Lock()
	o.BlockCache = cache
	o.mu.Unlock()
	return nil
}

func (o *Options) SetBlockCacheCapacity(capacity int) error {
	if o == nil {
		return ErrNotSet
	}
	o.mu.Lock()
	if o.BlockCache == nil {
		return ErrNotSet
	}
	o.BlockCache.SetCapacity(capacity)
	o.mu.Unlock()
	return nil
}

func (o *Options) SetBlockSize(size int) error {
	if o == nil {
		return ErrNotSet
	}
	if size <= 0 {
		return ErrInvalid
	}
	o.mu.Lock()
	o.BlockSize = size
	o.mu.Unlock()
	return nil
}

func (o *Options) SetBlockRestartInterval(interval int) error {
	if o == nil {
		return ErrNotSet
	}
	if interval <= 0 {
		return ErrInvalid
	}
	o.mu.Lock()
	o.BlockRestartInterval = interval
	o.mu.Unlock()
	return nil
}

func (o *Options) SetCompressionType(compression Compression) error {
	if o == nil {
		return ErrNotSet
	}
	if o.CompressionType >= nCompression {
		return ErrInvalid
	}
	o.mu.Lock()
	o.CompressionType = compression
	o.mu.Unlock()
	return nil
}

func (o *Options) SetFilter(p filter.Filter) error {
	if o == nil {
		return ErrNotSet
	}
	o.mu.Lock()
	o.Filter = p
	o.initFilters()
	o.filters[p.Name()] = p
	o.mu.Unlock()
	return nil
}

func (o *Options) InsertAltFilter(p filter.Filter) error {
	if o == nil {
		return ErrNotSet
	}
	o.mu.Lock()
	o.initFilters()
	o.filters[p.Name()] = p
	o.mu.Unlock()
	return nil
}

func (o *Options) RemoveAltFilter(name string) error {
	if o == nil {
		return ErrNotSet
	}
	o.mu.Lock()
	o.initFilters()
	delete(o.filters, name)
	o.mu.Unlock()
	return nil
}

func (o *Options) initFilters() {
	if o.filters == nil {
		o.filters = make(map[string]filter.Filter)
		for _, p := range o.AltFilters {
			o.filters[p.Name()] = p
		}
		if o.Filter != nil {
			o.filters[o.Filter.Name()] = o.Filter
		}
	}
}

type ReadOptionsFlag uint

const (
	// If true, all data read from underlying storage will be
	// verified against corresponding checksums.
	RFVerifyChecksums ReadOptionsFlag = 1 << iota

	// Should the data read for this iteration be cached in memory?
	// If set iteration chaching will be disabled.
	// Callers may wish to set this flag for bulk scans.
	RFDontFillCache
)

// ReadOptions represent sets of options used by LevelDB during read
// operations.
type ReadOptions struct {
	// Specify the read flag.
	Flag ReadOptionsFlag
}

type ReadOptionsGetter interface {
	HasFlag(flag ReadOptionsFlag) bool
}

func (o *ReadOptions) HasFlag(flag ReadOptionsFlag) bool {
	if o == nil {
		return false
	}
	return (o.Flag & flag) != 0
}

type WriteOptionsFlag uint

const (
	// If set, the write will be flushed from the operating system
	// buffer cache (by calling WritableFile::Sync()) before the write
	// is considered complete.  If this flag is true, writes will be
	// slower.
	//
	// If this flag is false, and the machine crashes, some recent
	// writes may be lost.  Note that if it is just the process that
	// crashes (i.e., the machine does not reboot), no writes will be
	// lost even if sync==false.
	//
	// In other words, a DB write with sync==false has similar
	// crash semantics as the "write()" system call.  A DB write
	// with sync==true has similar crash semantics to a "write()"
	// system call followed by "fsync()".
	WFSync WriteOptionsFlag = 1 << iota
)

// WriteOptions represent sets of options used by LevelDB during write
// operations.
type WriteOptions struct {
	// Specify the write flag.
	Flag WriteOptionsFlag
}

type WriteOptionsGetter interface {
	HasFlag(flag WriteOptionsFlag) bool
}

func (o *WriteOptions) HasFlag(flag WriteOptionsFlag) bool {
	if o == nil {
		return false
	}
	return (o.Flag & flag) != 0
}
