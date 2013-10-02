// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package leveldb provides implementation of LevelDB key/value database.
//
// Create or open a database:
//
//	db, err := leveldb.OpenFile("path/to/db", &opt.Options{Flag: opt.OFCreateIfMissing})
//	...
//	defer db.Close()
//	...
//
// Read or modify the database content:
//
//	ro := &opt.ReadOptions{}
//	wo := &opt.WriteOptions{}
//	// Remember that the contents of the returned slice should not be modified.
//	data, err := db.Get([]byte("key"), ro)
//	...
//	err = db.Put([]byte("key"), []byte("value"), wo)
//	...
//	err = db.Delete([]byte("key"), wo)
//	...
//
// Iterate over database content:
//
//	iter := db.NewIterator(ro)
//	for iter.Next() {
//		// Remember that the contents of the returned slice should not be modified, and
//		// only valid until the next call to Next.
//		key := iter.Key()
//		value := iter.Value()
//		...
//	}
//	iter.Release()
//	err = iter.Error()
//	...
//
// Batch writes:
//
//	batch := new(leveldb.Batch)
//	batch.Put([]byte("foo"), []byte("value"))
//	batch.Put([]byte("bar"), []byte("another value"))
//	batch.Delete([]byte("baz"))
//	err = db.Write(batch, wo)
//	...
//
// Use bloom filter:
//
//	o := &opt.Options{
//		Flag:   opt.OFCreateIfMissing,
//		Filter: filter.NewBloomFilter(10),
//	}
//	db, err := leveldb.OpenFile("path/to/db", o)
//	...
//	defer db.Close()
//	...
package leveldb
