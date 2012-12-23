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

type Reader interface {
	// Get value for given key.
	Get(key []byte, ro *ReadOptions) (value []byte, err error)

	// Return a heap-allocated iterator over the contents of the database.
	// The result of NewIterator() is initially invalid (caller must
	// call one of the Seek methods on the iterator before using it).
	//
	// Caller should delete the iterator when it is no longer needed.
	// The returned iterator should be deleted before this db is deleted.
	NewIterator(ro *ReadOptions) Iterator
}

type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type Writer interface {
	// Set the database entry for "key" to "value".
	Put(key, value []byte, wo *WriteOptions) error

	// Remove the database entry (if any) for "key". It is not an error
	// if "key" did not exist in the database.
	Delete(key []byte, wo *WriteOptions) error

	// Apply the specified updates to the database.
	Write(updates *Batch, wo *WriteOptions) error
}

type Snapshot interface {
	Reader

	// Release a previously acquired snapshot.  The caller must not
	// use "snapshot" after this call.
	Release()
}

type Range struct {
	// Start key, include in the range
	Start []byte

	// Limit, not include in the range
	Limit []byte
}

type DB interface {
	Reader
	Writer

	// Return a handle to the current DB state.  Iterators created with
	// this handle will all observe a stable snapshot of the current DB
	// state.  The caller must call ReleaseSnapshot(result) when the
	// snapshot is no longer needed.
	GetSnapshot() (snapshot Snapshot, err error)

	// DB implementations can export properties about their state
	// via this method.  If "property" is a valid property understood by this
	// DB implementation, then return the "value".
	//
	//
	// Valid property names include:
	//
	//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
	//     where <N> is an ASCII representation of a level number (e.g. "0").
	//  "leveldb.stats" - returns a multi-line string that describes statistics
	//     about the internal operation of the DB.
	//  "leveldb.sstables" - returns a multi-line string that describes all
	//     of the sstables that make up the db contents.
	GetProperty(property string) (value string, err error)

	// For each i in [0,n-1], store in "sizes[i]", the approximate
	// file system space used by keys in "[range[i].start .. range[i].limit)".
	//
	// Note that the returned sizes measure file system space usage, so
	// if the user data compresses by a factor of ten, the returned
	// sizes will be one-tenth the size of the corresponding user data size.
	//
	// The results may not include the sizes of recently written data.
	GetApproximateSizes(r *Range, n int) (size uint64, err error)

	// Compact the underlying storage for the key range [*begin,*end].
	// In particular, deleted and overwritten versions are discarded,
	// and the data is rearranged to reduce the cost of operations
	// needed to access the data.  This operation should typically only
	// be invoked by users who understand the underlying implementation.
	//
	// begin==nil is treated as a key before all keys in the database.
	// end==nil is treated as a key after all keys in the database.
	// Therefore the following call will compact the entire database:
	//    db.CompactRange(nil);
	CompactRange(r *Range) error

	// Close the DB.
	Close() error
}
