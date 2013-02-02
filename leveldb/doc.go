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

// Package leveldb provides implementation of LevelDB key/value database.
//
// Create or open a database:
//
//	mydesc, err := descriptor.OpenFile("path/to/db")
//	...
//	mydb, err := db.Open(mydesc, &opt.Options{Flag: opt.OFCreateIfMissing})
//	...
//
// Read or modify the database content:
//
//	ro := &opt.ReadOptions{}
//	wo := &opt.WriteOptions{}
//	data, err := mydb.Get([]byte("key"), ro)
//	...
//	err = mydb.Put([]byte("key"), []byte("value"), wo)
//	...
//	err = mydb.Delete([]byte("key"), wo)
//	...
//
// Iterate over database content:
//
//	myiter := mydb.NewIterator(ro)
//	for myiter.Next() {
//		key := myiter.Key()
//		value := myiter.Value()
//		...
//	}
//	err = myiter.Error()
//	...
//
// Batch writes:
//
//	batch := new(db.Batch)
//	batch.Put([]byte("foo"), []byte("value"))
//	batch.Put([]byte("bar"), []byte("another value"))
//	batch.Delete([]byte("baz"))
//	err = mydb.Write(batch, wo)
//	...
//
// Use bloom filter:
//
//	o := &opt.Options{
//		Flag:   opt.OFCreateIfMissing,
//		Filter: filter.NewBloomFilter(10),
//	}
//	mydb, err := db.Open(mydesc, o)
//	...
package leveldb
