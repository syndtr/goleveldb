# goLevelDB

Этот проект является реализацией [LevelDB key/value database](https://github.com/google/leveldb) на языке программирования [Go](https://go.dev).

[![Build Status](https://app.travis-ci.com/syndtr/goleveldb.svg?branch=master)](https://app.travis-ci.com/syndtr/goleveldb)

## Установка

```bash
go get github.com/syndtr/goleveldb/leveldb
```

## Требования

Необходим компилятор `go1.14` или новее.

## Использование

Cjplfybt yjdjq ,fps lfyys[]:

```go
// Возвращает новый экземпляр DB, безопасный для конкурентного использования. Which mean that all
// DB's methods may be called concurrently from multiple goroutine.
db, err := leveldb.OpenFile("path/to/db", nil)
...
defer db.Close()
...
```

Чтение или изменение содержимого базы данных:

```go
// Remember that the contents of the returned slice should not be modified.
data, err := db.Get([]byte("key"), nil)
...
err = db.Put([]byte("key"), []byte("value"), nil)
...
err = db.Delete([]byte("key"), nil)
...
```

Итерация по содержимому базы данных:

```go
iter := db.NewIterator(nil, nil)
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
```

Поиск через итерацию:

```go
iter := db.NewIterator(nil, nil)
for ok := iter.Seek(key); ok; ok = iter.Next() {
    // Use key/value.
    ...
}
iter.Release()
err = iter.Error()
...
```

Iterate over subset of database content:
Итерация по подмножеству ключей по содержимому базы данных:

```go
iter := db.NewIterator(&util.Range{Start: []byte("foo"), Limit: []byte("xoo")}, nil)
for iter.Next() {
    // Use key/value.
    ...
}
iter.Release()
err = iter.Error()
...
```

Итерация по подмножеству ключей с указанным префиксом:

```go
iter := db.NewIterator(util.BytesPrefix([]byte("foo-")), nil)
for iter.Next() {
    // Use key/value.
    ..
}
iter.Release()
err = iter.Error()
...
```

Пакетная запись

```go
batch := new(leveldb.Batch)
batch.Put([]byte("foo"), []byte("value"))
batch.Put([]byte("bar"), []byte("another value"))
batch.Delete([]byte("baz"))
err = db.Write(batch, nil)
...
```

С использованием фильтра Блума:

```go
o := &opt.Options{
    Filter: filter.NewBloomFilter(10),
}
db, err := leveldb.OpenFile("path/to/db", o)
...
defer db.Close()
...
```

## Документация

Вы можете прочитать документацию [здесь](https://pkg.go.dev/github.com/syndtr/goleveldb).
