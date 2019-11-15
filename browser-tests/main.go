// +build js

package main

import (
	"github.com/0xProject/qunit"
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	qunit.Module("LevelDB")
	qunit.Test("set and get", func(assert qunit.QUnitAssert) {
		dbPath := "leveldb/testing/db-" + uuid.New().String()
		db, err := leveldb.OpenFile(dbPath, nil)
		assertNoError(assert, err, "could not open db")

		key := []byte("foo")
		val := []byte("bar")
		err = db.Put(key, val, nil)
		assertNoError(assert, err, "could not put value")
		actual, err := db.Get(key, nil)
		assertNoError(assert, err, "could not get value")
		assert.Equal(string(actual), string(val), "got wrong value")

		// Close the database.
		err = db.Close()
		assertNoError(assert, err, "could not close db")

		// Re-open the database and the data should still be there.
		db, err = leveldb.OpenFile(dbPath, nil)
		assertNoError(assert, err, "could not open db")
		defer func() {
			err := db.Close()
			assertNoError(assert, err, "could not close db")
		}()
		actual, err = db.Get(key, nil)
		assertNoError(assert, err, "could not get value")
		assert.Equal(string(actual), string(val), "got wrong value")
	})

	qunit.Start()

	select {}
}

func assertNoError(assert qunit.QUnitAssert, err error, msg string) {
	if err != nil {
		assert.Ok(false, "unexpected error: "+err.Error())
	}
}
