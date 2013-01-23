This is an implementation of the [LevelDB key/value database](http:code.google.com/p/leveldb) in the [Go programming language](http:golang.org).

Installation
-----------

	go get github.com/syndtr/goleveldb/leveldb

Usage
-----------

Create or open a database:

	mydesc, err := desc.OpenFile("path/to/db")
	...
	mydb, err := db.Open(mydesc, &opt.Options{Flag: opt.OFCreateIfMissing})
	...

Read or modify the database content:

	ro := &opt.ReadOptions{}
	wo := &opt.WriteOptions{}
	data, err := mydb.Get([]byte("key"), ro)
	...
	err = mydb.Put([]byte("key"), []byte("value"), wo)
	...
	err = mydb.Delete([]byte("key"), wo)
	...

Iterate over database content:

	myiter := mydb.NewIterator(ro)
	for myiter.Next() {
		key := myiter.Key()
		value := myiter.Value()
		...
	}
	err = myiter.Error()
	...

Batch writes:

	batch := new(db.Batch)
	batch.Put([]byte("foo"), []byte("value"))
	batch.Put([]byte("bar"), []byte("another value"))
	batch.Delete([]byte("baz"))
	err = mydb.Write(batch, wo)
	...

Use bloom filter:

	o := &opt.Options{
		Flag:   opt.OFCreateIfMissing,
		Filter: filter.NewBloomFilter(10),
	}
	mydb, err := db.Open(mydesc, o)
	...
