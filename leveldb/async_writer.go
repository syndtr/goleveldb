// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

// asyncWriter wraps storage.Writer. It uses a background goroutine to perform real write op.
type asyncWriter struct {
	w        storage.Writer // raw writer
	sbpool   *simpleBufferPool
	bufSize  int
	commitC  chan interface{}
	doneC    chan struct{}
	errValue *atomic.Value

	buf []byte
}

func newAsyncWriter(w storage.Writer, bufSize, bufCount int, sbpool *simpleBufferPool) storage.Writer {
	commitC := make(chan interface{}, bufCount)
	doneC := make(chan struct{})
	var errValue atomic.Value

	// start the background data write goroutine
	go func() {
		defer close(doneC)
		for c := range commitC {
			if buf, ok := c.([]byte); ok {
				if errValue.Load() == nil {
					if _, err := w.Write(buf); err != nil {
						errValue.Store(err)
					}
				}
				sbpool.Put(buf)
			} else {
				// sync command
				ackC := c.(chan error)
				if err := errValue.Load(); err != nil {
					ackC <- err.(error)
				} else {
					ackC <- w.Sync()
				}
			}
		}
	}()

	return &asyncWriter{
		w:        w,
		sbpool:   sbpool,
		bufSize:  bufSize,
		commitC:  commitC,
		doneC:    doneC,
		errValue: &errValue,
	}
}

func (w *asyncWriter) Write(p []byte) (int, error) {
	for offset := 0; offset < len(p); {
		if l := len(w.buf); l > 0 && l == cap(w.buf) {
			// buf is full
			w.commitC <- w.buf
			w.buf = nil
		}

		if w.buf == nil {
			if err := w.errValue.Load(); err != nil {
				return 0, err.(error)
			}
			w.buf = w.sbpool.Get(w.bufSize)[:0]
		}

		n := minInt(cap(w.buf)-len(w.buf), len(p)-offset)
		w.buf = append(w.buf, p[offset:offset+n]...)
		offset += n
	}
	return len(p), nil
}

func (w *asyncWriter) Close() error {
	// flush remained buf
	if len(w.buf) > 0 {
		w.commitC <- w.buf
		w.buf = nil
	}
	close(w.commitC)
	// await data fully written
	<-w.doneC

	if err := w.w.Close(); err != nil {
		return err
	}
	if err := w.errValue.Load(); err != nil {
		return err.(error)
	}
	return nil
}

func (w *asyncWriter) Sync() error {
	// flush remained buf
	if len(w.buf) > 0 {
		w.commitC <- w.buf
		w.buf = nil
	}

	ackC := make(chan error)
	// send sync command
	w.commitC <- ackC
	return <-ackC
}
