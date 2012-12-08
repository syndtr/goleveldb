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

import (
	"testing"
)

func TestCache_NewId(t *testing.T) {
	c := NewLRUCache(1000, nil)
	a, b := c.NewId(), c.NewId()
	if a == b {
		t.Errorf("generated id are equal, %d == %d", a, b)
	}
}

func TestCache_HitMiss(t *testing.T) {
	cases := []struct{ key, value string }{
		{"a", "vvvvvvvvv"},
		{"k1", "v1"},
		{"xk", "v2"},
		{"bcdef", "v3"},
		{"fpqwe", "v4"},
		{"zzzz", "v5"},
		{"kx", "v6"},
		{"o", "v7"},
		{"z", "v8"},
		{"aaaaa", "v9"},
	}

	c := NewLRUCache(1000, nil)
	for i, x := range cases {
		c.Set([]byte(x.key), x.value, len(x.value)).Release()
		for j, y := range cases {
			r, ok := c.Get([]byte(y.key))
			if j <= i {
				// should hit
				if !ok {
					t.Errorf("case '%d' iteration '%d' is miss", i, j)
				}
				if r.Value().(string) != y.value {
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
		c.Delete([]byte(x.key))
		for j, y := range cases {
			r, ok := c.Get([]byte(y.key))
			if j > i {
				// should hit
				if !ok {
					t.Errorf("case '%d' iteration '%d' is miss", i, j)
				}
				if r.Value().(string) != y.value {
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
}

func TestLRUCache_Eviction(t *testing.T) {
	c := NewLRUCache(12, nil)
	c.Set([]byte("a"), "va", 1).Release()
	c.Set([]byte("b"), "vb", 1).Release()
	c.Set([]byte("c"), "vc", 1).Release()
	c.Set([]byte("d"), "vd", 1).Release()
	c.Set([]byte("e"), "ve", 1).Release()
	r, _ := c.Get([]byte("b"))
	r.Release()
	c.Set([]byte("x"), "vx", 10).Release()

	for _, x := range []string{"b", "e", "x"} {
		r, ok := c.Get([]byte(x))
		if !ok {
			t.Errorf("miss for key '%s'", x)
		} else {
			if r.Value().(string) != "v"+x {
				t.Errorf("invalid value for key '%s' want 'v%s', got '%s'", x, x, r.Value().(string))
			}
			r.Release()
		}
	}

	for _, x := range []string{"a", "c", "d"} {
		r, ok := c.Get([]byte(x))
		if ok {
			t.Errorf("hit for key '%s'", x)
			if r.Value().(string) != "v"+x {
				t.Errorf("invalid value for key '%s' want 'v%s', got '%s'", x, x, r.Value().(string))
			}
			r.Release()
		}
	}
}
