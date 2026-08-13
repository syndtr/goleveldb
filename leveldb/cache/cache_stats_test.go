// Copyright (c) 2026, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync"
	"testing"
)

// TestGetStatsAfterClose covers a nil dereference in GetStats: Close clears
// mHead, and GetStats read len(mHead.buckets) without the closed check every
// other exported method takes.
//
// The reachable path is leveldb.(*DB).Stats, which checks db.ok() first and
// then reads the file cache's statistics; a Close landing in between panicked
// the process.
func TestGetStatsAfterClose(t *testing.T) {
	c := NewCache(NewLRU(2))
	c.Close(false)

	stats := c.GetStats()
	if stats.Buckets != 0 {
		t.Errorf("Buckets = %d after close, want 0", stats.Buckets)
	}
}

// TestGetStatsConcurrentClose exercises the same path under the race detector,
// where the window between the nil check and the read is what matters.
func TestGetStatsConcurrentClose(t *testing.T) {
	c := NewCache(NewLRU(2))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = c.GetStats()
		}
	}()
	go func() {
		defer wg.Done()
		c.Close(false)
	}()
	wg.Wait()
}
