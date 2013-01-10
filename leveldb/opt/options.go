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

// Package opt provides sets of options used by LevelDB.
package opt

import (
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/filter"
	"sync"
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
	// Default: a comparer that uses lexicographic byte-wise ordering
	//
	// REQUIRES: The client must ensure that the comparer supplied
	// here has the same name and orders keys *exactly* the same as the
	// comparer provided to previous open calls on the same DB.
	Comparer comparer.Comparer

	// Specify the database flag.
	Flag OptionsFlag

	// Amount of data to build up in memory (backed by an unsorted log
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
	// Default: NULL
	Filter filter.Filter

	mu sync.Mutex
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
}

func (o *Options) GetComparer() comparer.Comparer {
	if o == nil || o.Comparer == nil {
		return comparer.DefaultComparer
	}
	return o.Comparer
}

func (o *Options) HasFlag(flag OptionsFlag) bool {
	if o == nil {
		return false
	}
	return (o.Flag & flag) != 0
}

func (o *Options) GetWriteBuffer() int {
	if o == nil || o.WriteBuffer <= 0 {
		return 4 << 20
	}
	return o.WriteBuffer
}

func (o *Options) GetMaxOpenFiles() int {
	if o == nil || o.MaxOpenFiles <= 0 {
		return 1000
	}
	return o.MaxOpenFiles
}

func (o *Options) GetBlockCache() cache.Cache {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o == nil {
		return nil
	}
	if o.BlockCache == nil {
		o.BlockCache = cache.NewLRUCache(8 << 20)
	}
	return o.BlockCache
}

func (o *Options) GetBlockSize() int {
	if o == nil || o.BlockSize <= 0 {
		return 4096
	}
	return o.BlockSize
}

func (o *Options) GetBlockRestartInterval() int {
	if o == nil || o.BlockRestartInterval <= 0 {
		return 16
	}
	return o.BlockRestartInterval
}

func (o *Options) GetCompressionType() Compression {
	if o == nil || o.CompressionType <= DefaultCompression || o.CompressionType >= nCompression {
		return SnappyCompression
	}
	return o.CompressionType
}

func (o *Options) GetFilter() filter.Filter {
	if o == nil {
		return nil
	}
	return o.Filter
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
