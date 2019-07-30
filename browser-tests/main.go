// +build js

package main

import (
	"github.com/0xProject/qunit"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	qunit.Module("LevelDB")
	qunit.Test("set and get", func(assert qunit.QUnitAssert) {
		db, err := leveldb.OpenFile(".", nil)
		assert.Ok(err == nil, "could not open db: "+err.Error())
		key := []byte("foo")
		val := []byte("bar")
		err = db.Put(key, val, nil)
		assert.Ok(err == nil, "could not put value: "+err.Error())
		actual, err := db.Get(key, nil)
		assert.Ok(err == nil, "could not get value: "+err.Error())
		assert.Equal(actual, val, "got wrong value")
	})

	qunit.Start()

	select {}
}
