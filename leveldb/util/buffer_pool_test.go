// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package util

import (
	"runtime"
	"testing"
	"time"
)

func checkClosed(ch chan []byte) (closed bool) {
	defer func() {
		x := recover()
		if err, ok := x.(error); ok && err.Error() == "send on closed channel" {
			closed = true
		}
	}()
	select {
	case ch <- nil:
	default:
	}
	return
}

func TestBufferPoolCloseByGC(t *testing.T) {
	bpool := NewBufferPool(1024)
	pool := bpool.pool // ref to check closed
	buf := bpool.Get(1024)
	if 1024 != len(buf) {
		t.Errorf("Get() return invalid length buffer, got(%v), expect(%v)", len(buf), 1024)
	}
	bpool.Put(buf)
	bpool = nil
	runtime.GC()
	time.Sleep(time.Second)
	for i, ch := range pool {
		if !checkClosed(ch) {
			t.Errorf("pool[%d] should be closed after GC", i)
		}
	}
}
