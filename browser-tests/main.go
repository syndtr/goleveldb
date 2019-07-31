// +build js

package main

import (
	"github.com/0xProject/qunit"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	qunit.Module("LevelDB")
	qunit.Test("set and get", func(assert qunit.QUnitAssert) {
		db, err := leveldb.OpenFile("leveldb-testing", nil)
		if err != nil {
			assert.Ok(false, "could not open db: "+err.Error())
		}
		key := []byte("foo")
		val := []byte("bar")
		err = db.Put(key, val, nil)
		if err != nil {
			assert.Ok(false, "could not put value: "+err.Error())
		}
		actual, err := db.Get(key, nil)
		if err != nil {
			assert.Ok(false, "could not get value: "+err.Error())
		}
		assert.Equal(string(actual), string(val), "got wrong value")
	})

	qunit.Start()

	select {}
}
