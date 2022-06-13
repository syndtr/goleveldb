// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync/atomic"
	"testing"
)

func BenchmarkCache_InsertRemove(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		}).Release()
	}
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCache_Insert(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCache_Lookup(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), nil).Release()
	}
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCache_AppendRemove(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Get(1, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		}).Release()
	}
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCache_Append(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Get(1, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCache_Delete(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)
	handles := make([]*Handle, b.N)
	for i := 0; i < b.N; i++ {
		handles[i] = c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		handles[i].Release()
	}
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCacheParallel_Insert(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)

	var ns uint64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		ns := atomic.AddUint64(&ns, 1)
		i := uint64(0)
		for pb.Next() {
			c.Get(ns, i, func() (int, Value) {
				return 1, i
			})
			i++
		}
	})
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCacheParallel_Lookup(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}

	var counter uint64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&counter, 1) - 1
			c.Get(0, i, nil).Release()
		}
	})
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCacheParallel_Append(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)
	for i := 0; i < b.N; i++ {
		c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}

	var ns uint64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		ns := atomic.AddUint64(&ns, 1)
		i := uint64(0)
		for pb.Next() {
			c.Get(ns, i, func() (int, Value) {
				return 1, i
			})
			i++
		}
	})
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}

func BenchmarkCacheParallel_Delete(b *testing.B) {
	b.StopTimer()
	c := NewCache(nil)
	handles := make([]*Handle, b.N)
	for i := 0; i < b.N; i++ {
		handles[i] = c.Get(0, uint64(i), func() (int, Value) {
			return 1, uint64(i)
		})
	}

	var counter int64
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1) - 1
			handles[i].Release()
		}
	})
	b.ReportMetric(float64(c.Nodes()), "nodes")
	b.Logf("STATS: %#v", c.GetStats())
}
