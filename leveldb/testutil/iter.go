// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package testutil

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	IterNone = iota
	IterFirst
	IterLast
	IterPrev
	IterNext
	IterSeek
	IterSOI
	IterEOI
)

type IteratorTesting struct {
	*testing.T
	KeyValue
	Iter    iterator.Iterator
	Rand    *rand.Rand
	PostFn  func(t *IteratorTesting)
	Pos     int
	LastAct int

	once bool
}

func (t *IteratorTesting) init() {
	if !t.once {
		t.Pos = -1
		t.once = true
	}
}

func (t *IteratorTesting) post() {
	if t.PostFn != nil {
		t.PostFn(t)
	}
}

func (t *IteratorTesting) IsFirst() bool {
	t.init()
	return t.Len() > 0 && t.Pos == 0
}

func (t *IteratorTesting) IsLast() bool {
	t.init()
	return t.Len() > 0 && t.Pos == t.Len()-1
}

func (t *IteratorTesting) TestKV() {
	t.init()
	key, value := t.Index(t.Pos)
	if !bytes.Equal(key, t.Iter.Key()) {
		t.Errorf("invalid key = %q want %q", key, t.Iter.Key())
	}
	if !bytes.Equal(value, t.Iter.Value()) {
		t.Errorf("invalid value = %q want %q", value, t.Iter.Value())
	}
}

func (t *IteratorTesting) First() {
	t.init()
	t.LastAct = IterFirst
	if t.Len() > 0 {
		t.Pos = 0
		if !t.Iter.First() {
			t.Fatalf("First: got soi, err=%v", t.Iter.Error())
		}
		t.TestKV()
	} else {
		t.Pos = -1
		if t.Iter.First() {
			t.Fatalf("First: got %q expected soi, err=%v", t.Iter.Key(), t.Iter.Error())
		}
	}
	t.post()
}

func (t *IteratorTesting) Last() {
	t.init()
	t.LastAct = IterLast
	if t.Len() > 0 {
		t.Pos = t.Len() - 1
		if !t.Iter.Last() {
			t.Fatalf("Last: got eoi, err=%v", t.Iter.Error())
		}
		t.TestKV()
	} else {
		t.Pos = 0
		if t.Iter.Last() {
			t.Fatalf("Last: got %q expected eoi, err=%v", t.Iter.Key(), t.Iter.Error())
		}
	}
	t.post()
}

func (t *IteratorTesting) Next() {
	t.init()
	t.LastAct = IterNext
	if t.Pos < t.Len()-1 {
		t.Pos++
		if !t.Iter.Next() {
			t.Fatalf("Next: got eoi, pos=%d err=%v", t.Pos, t.Iter.Error())
		}
		t.TestKV()
	} else {
		t.Pos = t.Len()
		if t.Iter.Next() {
			t.Fatalf("Next: got %q expected eoi, err=%v", t.Iter.Key(), t.Iter.Error())
		}
	}
	t.post()
}

func (t *IteratorTesting) Prev() {
	t.init()
	t.LastAct = IterPrev
	if t.Pos > 0 {
		t.Pos--
		if !t.Iter.Prev() {
			t.Fatalf("Prev: got soi, pos=%d err=%v", t.Pos, t.Iter.Error())
		}
		t.TestKV()
	} else {
		t.Pos = -1
		if t.Iter.Prev() {
			t.Fatalf("Prev: got %q expected soi, err=%v", t.Iter.Key(), t.Iter.Error())
		}
	}
	t.post()
}

func (t *IteratorTesting) Seek(i int) {
	t.init()
	t.LastAct = IterSeek
	key, _ := t.Index(i)
	oldKey, _ := t.IndexOrNil(t.Pos)
	t.Pos = i
	if !t.Iter.Seek(key) {
		t.Fatalf("Seek %q -> %q: got eoi, pos=%d err=%v", oldKey, key, t.Pos, t.Iter.Error())
	}
	t.TestKV()
	t.post()
}

func (t *IteratorTesting) SeekInexact(i int) {
	t.init()
	t.LastAct = IterSeek
	var key0 []byte
	key1, _ := t.Index(i)
	if i > 0 {
		key0, _ = t.Index(i - 1)
	}
	key := BytesSeparator(key0, key1)
	oldKey, _ := t.IndexOrNil(t.Pos)
	t.Pos = i
	if !t.Iter.Seek(key) {
		t.Fatalf("SeekInexact %q -> %q: got eoi, pos=%d key=%q err=%v", oldKey, key, key1, t.Pos, t.Iter.Error())
	}
	t.TestKV()
	t.post()
}

func (t *IteratorTesting) SeekKey(key []byte) {
	t.init()
	t.LastAct = IterSeek
	oldKey, _ := t.IndexOrNil(t.Pos)
	t.Pos = t.Search(key)
	if t.Pos < t.Len() {
		key_, _ := t.Index(t.Pos)
		if !t.Iter.Seek(key) {
			t.Fatalf("Seek %q -> %q: got eoi, key=%q pos=%d err=%v", oldKey, key, key_, t.Pos, t.Iter.Error())
		}
		t.TestKV()
	} else {
		if t.Iter.Seek(key) {
			t.Fatalf("Seek %q -> %q: got %q expected eoi, err=%v", oldKey, key, t.Iter.Key(), t.Iter.Error())
		}
	}
	t.post()
}

func (t *IteratorTesting) SOI() {
	t.init()
	t.LastAct = IterSOI
	if t.Pos > 0 {
		t.Fatalf("SOI: unmet condition, pos > 0")
	}
	for i := 0; i < 3; i++ {
		t.Prev()
	}
	t.post()
}

func (t *IteratorTesting) EOI() {
	t.init()
	t.LastAct = IterEOI
	if t.Pos < t.Len()-1 {
		t.Fatalf("EOI: unmet condition, pos < %d", t.Len()-1)
	}
	for i := 0; i < 3; i++ {
		t.Next()
	}
	t.post()
}

func (t *IteratorTesting) WalkPrev(fn func(t *IteratorTesting)) {
	t.init()
	for old := t.Pos; t.Pos > 0; old = t.Pos {
		fn(t)
		if t.Pos >= old {
			t.Fatalf("WalkPrev: pos not regressing %d -> %d", old, t.Pos)
		}
	}
}

func (t *IteratorTesting) WalkNext(fn func(t *IteratorTesting)) {
	t.init()
	for old := t.Pos; t.Pos < t.Len()-1; old = t.Pos {
		fn(t)
		if t.Pos <= old {
			t.Fatalf("WalkNext: pos not advancing %d -> %d", old, t.Pos)
		}
	}
}

func (t *IteratorTesting) PrevAll() {
	t.WalkPrev(func(t *IteratorTesting) {
		t.Prev()
	})
}

func (t *IteratorTesting) NextAll() {
	t.WalkNext(func(t *IteratorTesting) {
		t.Next()
	})
}

func (t *IteratorTesting) Test() {
	if t.Rand == nil {
		seed := time.Now().UnixNano()
		t.Rand = rand.New(rand.NewSource(seed))
		t.Logf("IteratorTesting: seed %d", seed)
	}

	t.SOI()
	t.NextAll()
	t.First()
	t.SOI()
	t.NextAll()
	t.EOI()
	t.PrevAll()
	t.Last()
	t.EOI()
	t.PrevAll()
	t.SOI()

	t.NextAll()
	t.PrevAll()
	t.NextAll()
	t.Last()
	t.PrevAll()
	t.First()
	t.NextAll()
	t.EOI()

	// t.Log("IteratorTesting: shuffled seeks")
	ShuffledIndex(t.Rand, t.Len(), 1, func(i int) {
		t.Seek(i)
	})
	ShuffledIndex(t.Rand, t.Len(), 1, func(i int) {
		t.SeekInexact(i)
	})
	ShuffledIndex(t.Rand, t.Len(), 1, func(i int) {
		t.Seek(i)
		if i%2 != 0 {
			t.PrevAll()
			t.SOI()
		} else {
			t.NextAll()
			t.EOI()
		}
	})

	for _, key := range []string{"", "foo", "bar", "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"} {
		t.SeekKey([]byte(key))
	}
}
