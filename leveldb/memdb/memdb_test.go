// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package memdb

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

func TestPutDelete(t *testing.T) {
	p := New(comparer.DefaultComparer, 0)

	assertExist := func(key string, want bool) {
		got := p.Contains([]byte(key))
		if got != want {
			if got {
				t.Errorf("key %q exist", key)
			} else {
				t.Errorf("key %q doesn't exist", key)
			}
		}
	}

	assertLen := func(want int) {
		got := p.Len()
		if got != want {
			t.Errorf("invalid length, want=%d got=%d", want, got)
		}
	}

	assertSize := func(want int) {
		got := p.Size()
		if got != want {
			t.Errorf("invalid size, want=%d got=%d", want, got)
		}
	}

	assertFind := func(key string, want_found bool, want_key string) {
		rkey, _, err := p.Find([]byte(key))
		if err == nil {
			if !want_found {
				t.Errorf("found key: %q", string(rkey))
			} else if want_key != string(rkey) {
				t.Errorf("invalid key, want=%q got=%q", want_key, string(rkey))
			}
		} else if want_found {
			t.Errorf("key %q not found", key)
		}
	}

	assertLen(0)
	assertSize(0)
	assertExist("", false)
	assertFind("", false, "")
	p.Put([]byte("foo"), nil)
	assertLen(1)
	assertSize(3)
	assertExist("foo", true)
	assertExist("bar", false)
	assertFind("foo", true, "foo")
	assertFind("bar", true, "foo")
	p.Put([]byte("bar"), []byte("xx"))
	assertLen(2)
	assertSize(8)
	assertExist("bar", true)
	p.Put([]byte("bar"), []byte("xxx"))
	assertLen(2)
	assertSize(9)
	assertExist("bar", true)
	p.Put([]byte("bar"), []byte(""))
	assertSize(6)
	p.Delete([]byte("foo"))
	assertLen(1)
	assertExist("foo", false)
	p.Delete([]byte("foo"))
	assertLen(1)
	assertSize(3)
	assertExist("bar", true)
	assertFind("zz", false, "")
	p.Put([]byte("zz"), nil)
	assertLen(2)
	assertSize(5)
	assertExist("zz", true)
	p.Delete([]byte("bar"))
	assertExist("bar", false)
	assertFind("bar", true, "zz")
	assertExist("zz", true)
	p.Delete([]byte("bar"))
	assertExist("zz", true)
	p.Delete([]byte("zz"))
	assertExist("zz", false)
	assertFind("zz", false, "")
	assertLen(0)
	assertSize(0)
}

func BenchmarkPut(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	b.ResetTimer()
	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}
}

func BenchmarkPutRandom(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	b.ResetTimer()
	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}

	b.ResetTimer()
	for i := range buf {
		p.Get(buf[i][:])
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		p.Put(buf[i][:], nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Get(buf[rand.Int()%b.N][:])
	}
}
