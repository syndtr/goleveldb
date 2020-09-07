// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// asyncIterator wraps the iterator. It uses a background goroutine to perform raw iteration.
type asyncIterator struct {
	iter     iterator.Iterator
	sbpool   *simpleBufferPool
	commitC  chan []byte
	releaseC chan struct{}

	buf    []byte
	offset int
	k, v   []byte
}

func newAsyncIterator(iter iterator.Iterator, bufSize, bufCount int, sbpool *simpleBufferPool) *asyncIterator {
	commitC := make(chan []byte, bufCount)
	releaseC := make(chan struct{})

	// start the goroutine to iterate on raw iterator and produce bufs.
	go func() {
		defer close(commitC)

		var buf []byte
		for iter.Next() {
			k, v := iter.Key(), iter.Value()
			klen, vlen := len(k), len(v)
			// new kv will exceed the buf
			if kvLen, bufLen := 8+klen+vlen, len(buf); bufLen+kvLen > cap(buf) {
				select {
				case <-releaseC:
					if buf != nil {
						sbpool.Put(buf)
					}
					return
				default:
				}
				if bufLen > 0 {
					// commit the buf if any
					commitC <- buf
				}
				// then alloc a new one
				buf = sbpool.Get(maxInt(bufSize, kvLen))[:0]
			}

			// encode key value as [ klen | vlen | key | value ]
			o := len(buf)
			buf = buf[:o+8] // extend the buf for kv len
			binary.LittleEndian.PutUint32(buf[o:], uint32(klen))
			binary.LittleEndian.PutUint32(buf[o+4:], uint32(vlen))
			buf = append(buf, k...)
			buf = append(buf, v...)
		}
		// commit the remained
		if len(buf) > 0 {
			commitC <- buf
		}
	}()

	return &asyncIterator{
		iter:     iter,
		sbpool:   sbpool,
		commitC:  commitC,
		releaseC: releaseC,
	}
}

func (i *asyncIterator) Next() bool {
	i.k, i.v = nil, nil

	if len(i.buf) == i.offset {
		// the buf is fully consumed

		// return the buf to pool
		if cap(i.buf) > 0 {
			i.sbpool.Put(i.buf)
		}
		// read a new buf
		if i.buf = <-i.commitC; i.buf == nil {
			// end
			return false
		}
		i.offset = 0
	}

	klen := int(binary.LittleEndian.Uint32(i.buf[i.offset:]))
	vlen := int(binary.LittleEndian.Uint32(i.buf[i.offset+4:]))
	i.offset += 8
	i.k = i.buf[i.offset : i.offset+klen]
	i.offset += klen
	i.v = i.buf[i.offset : i.offset+vlen]
	i.offset += vlen
	return true
}

func (i *asyncIterator) Key() []byte   { return i.k }
func (i *asyncIterator) Value() []byte { return i.v }
func (i *asyncIterator) Error() error  { return i.iter.Error() }

func (i *asyncIterator) Release() {
	close(i.releaseC)
	// drain committed bufs
	for buf := range i.commitC {
		i.sbpool.Put(buf)
	}
	i.iter.Release()
}
