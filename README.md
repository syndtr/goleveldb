This is an implementation of the [LevelDB key/value database](http:code.google.com/p/leveldb) in the [Go programming language](http:golang.org).

Installation
-----------

	go get github.com/syndtr/goleveldb/leveldb

Usage
-----------

Create or open a database:

	db, err := leveldb.OpenFile("path/to/db", nil)
	...
	defer db.Close()
	...

Read or modify the database content:

	// Remember that the contents of the returned slice should not be modified.
	data, err := db.Get([]byte("key"), nil)
	...
	err = db.Put([]byte("key"), []byte("value"), nil)
	...
	err = db.Delete([]byte("key"), nil)
	...

Iterate over database content:

	iter := db.NewIterator(nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()
		...
	}
	iter.Release()
	err = iter.Error()
	...

Batch writes:

	batch := new(leveldb.Batch)
	batch.Put([]byte("foo"), []byte("value"))
	batch.Put([]byte("bar"), []byte("another value"))
	batch.Delete([]byte("baz"))
	err = db.Write(batch, nil)
	...

Use bloom filter:

	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile("path/to/db", o)
	...
	defer db.Close()
	...

Documentation
-----------

You can read package documentation [here](http:godoc.org/github.com/syndtr/goleveldb).
