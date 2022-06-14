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

func BenchmarkPut(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	b.ResetTimer()
	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		if err := p.Put(buf[i][:], nil); err != nil {
			b.Fatal(err)
		}
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
		if err := p.Put(buf[i][:], nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		if err := p.Put(buf[i][:], nil); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := range buf {
		if _, err := p.Get(buf[i][:]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][4]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
	}

	p := New(comparer.DefaultComparer, 0)
	for i := range buf {
		if err := p.Put(buf[i][:], nil); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Get(buf[rand.Int()%b.N][:]); err != nil {
			b.Fatal(err)
		}
	}
}
