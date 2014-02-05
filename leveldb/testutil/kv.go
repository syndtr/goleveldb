// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package testutil

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/syndtr/goleveldb/leveldb/util"
)

type KeyValue struct {
	keys, values [][]byte
}

func (kv *KeyValue) Put(key, value []byte) {
	if n := len(kv.keys); n > 0 && cmp.Compare(kv.keys[n-1], key) >= 0 {
		panic(fmt.Sprintf("Put: keys are not in increasing order: %q, %q", kv.keys[n-1], key))
	}
	kv.keys = append(kv.keys, key)
	kv.values = append(kv.values, value)
}

func (kv *KeyValue) PutString(key, value string) {
	kv.Put([]byte(key), []byte(value))
}

func (kv KeyValue) Len() int {
	return len(kv.keys)
}

func (kv KeyValue) Index(i int) (key, value []byte) {
	if i < 0 || i >= len(kv.keys) {
		panic(fmt.Sprintf("Index #%d: out of range", i))
	}
	return kv.keys[i], kv.values[i]
}

func (kv KeyValue) IndexInexact(i int) (key_, key, value []byte) {
	key, value = kv.Index(i)
	var key0 []byte
	var key1 = kv.keys[i]
	if i > 0 {
		key0 = kv.keys[i-1]
	}
	key_ = BytesSeparator(key0, key1)
	return
}

func (kv KeyValue) IndexOrNil(i int) (key, value []byte) {
	if i >= 0 && i < len(kv.keys) {
		return kv.keys[i], kv.values[i]
	}
	return nil, nil
}

func (kv KeyValue) IndexString(i int) (key, value string) {
	key_, _value := kv.Index(i)
	return string(key_), string(_value)
}

func (kv KeyValue) Search(key []byte) int {
	return sort.Search(kv.Len(), func(i int) bool {
		key_, _ := kv.Index(i)
		return cmp.Compare(key_, key) >= 0
	})
}

func (kv KeyValue) SearchString(key string) int {
	return kv.Search([]byte(key))
}

func (kv KeyValue) Iterate(fn func(i int, key, value []byte)) {
	for i := range kv.keys {
		fn(i, kv.keys[i], kv.values[i])
	}
}

func (kv KeyValue) IterateString(fn func(i int, key, value string)) {
	kv.Iterate(func(i int, key, value []byte) {
		fn(i, string(key), string(value))
	})
}

func (kv KeyValue) IterateShuffled(rnd *rand.Rand, fn func(i int, key, value []byte)) (seed int64) {
	return ShuffledIndex(rnd, kv.Len(), 1, func(i int) {
		fn(i, kv.keys[i], kv.values[i])
	})
}

func (kv KeyValue) IterateShuffledString(rnd *rand.Rand, fn func(i int, key, value string)) (seed int64) {
	return kv.IterateShuffled(rnd, func(i int, key, value []byte) {
		fn(i, string(key), string(value))
	})
}

func (kv KeyValue) IterateInexact(fn func(i int, key_, key, value []byte)) {
	for i := range kv.keys {
		key_, key, value := kv.IndexInexact(i)
		fn(i, key_, key, value)
	}
}

func (kv KeyValue) IterateInexactString(fn func(i int, key_, key, value string)) {
	kv.IterateInexact(func(i int, key_, key, value []byte) {
		fn(i, string(key_), string(key), string(value))
	})
}

func (kv KeyValue) Clone() KeyValue {
	return KeyValue{append([][]byte{}, kv.keys...), append([][]byte{}, kv.values...)}
}

func (kv KeyValue) Slice(start, limit int) KeyValue {
	if start < 0 || limit > kv.Len() {
		panic(fmt.Sprintf("Slice %d .. %d: out of range", start, limit))
	} else if limit < start {
		panic(fmt.Sprintf("Slice %d .. %d: invalid range", start, limit))
	}
	return KeyValue{append([][]byte{}, kv.keys[start:limit]...), append([][]byte{}, kv.values[start:limit]...)}
}

func (kv KeyValue) SliceKey(start, limit []byte) KeyValue {
	start_ := 0
	limit_ := kv.Len()
	if start != nil {
		start_ = kv.Search(start)
	}
	if limit != nil {
		limit_ = kv.Search(limit)
	}
	return kv.Slice(start_, limit_)
}

func (kv KeyValue) SliceKeyString(start, limit string) KeyValue {
	return kv.SliceKey([]byte(start), []byte(limit))
}

func (kv KeyValue) SliceRange(r *util.Range) KeyValue {
	if r != nil {
		return kv.SliceKey(r.Start, r.Limit)
	}
	return kv.Clone()
}

func (kv KeyValue) Range(start, limit int) (r util.Range) {
	if kv.Len() > 0 {
		if start == kv.Len() {
			r.Start = BytesAfter(kv.keys[start-1])
		} else {
			r.Start = kv.keys[start]
		}
	}
	if limit < kv.Len() {
		r.Limit = kv.keys[limit]
	}
	return
}
