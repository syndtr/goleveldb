// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This LevelDB Go implementation is based on LevelDB C++ implementation.
// Which contains the following header:
//   Copyright (c) 2011 The LevelDB Authors. All rights reserved.
//   Use of this source code is governed by a BSD-style license that can be
//   found in the LEVELDBCPP_LICENSE file. See the LEVELDBCPP_AUTHORS file
//   for names of contributors.

package leveldb

import "testing"

func TestCache_HitMiss(t *testing.T) {
	cases := []struct {
		key   uint64
		value string
	}{
		{1, "vvvvvvvvv"},
		{100, "v1"},
		{0, "v2"},
		{12346, "v3"},
		{777, "v4"},
		{999, "v5"},
		{7654, "v6"},
		{2, "v7"},
		{3, "v8"},
		{9, "v9"},
	}

	setfin := 0
	c := NewLRUCache(1000)
	ns := c.GetNamespace(0)
	for i, x := range cases {
		ns.Set(x.key, x.value, len(x.value), func() {
			setfin++
		}).Release()
		for j, y := range cases {
			r, ok := ns.Get(y.key)
			if j <= i {
				// should hit
				if !ok {
					t.Errorf("case '%d' iteration '%d' is miss", i, j)
				} else if r.Value().(string) != y.value {
					t.Errorf("case '%d' iteration '%d' has invalid value got '%s', want '%s'", i, j, r.Value().(string), y.value)
				}
			} else {
				// should miss
				if ok {
					t.Errorf("case '%d' iteration '%d' is hit , value '%s'", i, j, r.Value().(string))
				}
			}
			if ok {
				r.Release()
			}
		}
	}

	for i, x := range cases {
		finalizerOk := false
		ns.Delete(x.key, func() {
			finalizerOk = true
		})

		if !finalizerOk {
			t.Errorf("case %d delete finalizer not executed", i)
		}

		for j, y := range cases {
			r, ok := ns.Get(y.key)
			if j > i {
				// should hit
				if !ok {
					t.Errorf("case '%d' iteration '%d' is miss", i, j)
				} else if r.Value().(string) != y.value {
					t.Errorf("case '%d' iteration '%d' has invalid value got '%s', want '%s'", i, j, r.Value().(string), y.value)
				}
			} else {
				// should miss
				if ok {
					t.Errorf("case '%d' iteration '%d' is hit, value '%s'", i, j, r.Value().(string))
				}
			}
			if ok {
				r.Release()
			}
		}
	}

	if setfin != len(cases) {
		t.Errorf("some set finalizer may not not executed, want=%d got=%d", len(cases), setfin)
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	c := NewLRUCache(12)
	ns := c.GetNamespace(0)
	ns.Set(1, 1, 1, nil).Release()
	ns.Set(2, 2, 1, nil).Release()
	ns.Set(3, 3, 1, nil).Release()
	ns.Set(4, 4, 1, nil).Release()
	ns.Set(5, 5, 1, nil).Release()
	if r, ok := ns.Get(2); ok {
		r.Release()
	}
	ns.Set(9, 9, 10, nil).Release()
	// 	c.esched <- true

	for _, x := range []uint64{2, 5, 9} {
		r, ok := ns.Get(x)
		if !ok {
			t.Errorf("miss for key '%d'", x)
		} else {
			if r.Value().(int) != int(x) {
				t.Errorf("invalid value for key '%d' want '%d', got '%d'", x, x, r.Value().(int))
			}
			r.Release()
		}
	}

	for _, x := range []uint64{1, 3, 4} {
		r, ok := ns.Get(x)
		if ok {
			t.Errorf("hit for key '%d'", x)
			if r.Value().(int) != int(x) {
				t.Errorf("invalid value for key '%d' want '%d', got '%d'", x, x, r.Value().(int))
			}
			r.Release()
		}
	}
}

func BenchmarkLRUCache_SetRelease(b *testing.B) {
	capacity := b.N / 100
	if capacity <= 0 {
		capacity = 10
	}
	c := NewLRUCache(capacity)
	ns := c.GetNamespace(0)
	b.ResetTimer()
	for i := uint64(0); i < uint64(b.N); i++ {
		ns.Set(i, nil, 1, nil).Release()
	}
}
