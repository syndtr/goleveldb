This is an implementation of the [LevelDB key/value database](http://code.google.com/p/leveldb) in the [Go programming language](http://golang.org).

Installation
-----------

	go get github.com/syndtr/goleveldb/leveldb

Usage
-----------

Create or open a database:

	desc, err := descriptor.OpenFile("path/to/db")
	...
	db, err := leveldb.Open(desc, &opt.Options{Flag: opt.OFCreateIfMissing})
	...

Closing the database also incolves closing its descriptor:

	...
	err := db.Close()
	desc.Close()

Read or modify the database content:

	ro := &opt.ReadOptions{}
	wo := &opt.WriteOptions{}
	data, err := db.Get([]byte("key"), ro)
	...
	err = db.Put([]byte("key"), []byte("value"), wo)
	...
	err = db.Delete([]byte("key"), wo)
	...

Iterate over database content:

	iter := db.NewIterator(ro)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		...
	}
	err = iter.Error()
	...

Batch writes:

	batch := new(leveldb.Batch)
	batch.Put([]byte("foo"), []byte("value"))
	batch.Put([]byte("bar"), []byte("another value"))
	batch.Delete([]byte("baz"))
	err = db.Write(batch, wo)
	...

Use bloom filter:

	o := &opt.Options{
		Flag:   opt.OFCreateIfMissing,
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.Open(desc, o)
	...

Documentation
-----------

You can read package documentation [here](http://godoc.org/github.com/syndtr/goleveldb).
