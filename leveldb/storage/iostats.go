// Copyright (c) 2018, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"sync/atomic"
)

// IOStats holds the cumulative number of read and written bytes of the underlying storage.
type IOStats struct {
	read  uint64
	write uint64
}

// AddRead increases the number of read bytes by n.
func (s *IOStats) AddRead(n uint64) uint64 {
	return atomic.AddUint64(&s.read, n)
}

// AddWrite increases the number of written bytes by n.
func (s *IOStats) AddWrite(n uint64) uint64 {
	return atomic.AddUint64(&s.write, n)
}

// Reads returns the number of read bytes.
func (s *IOStats) Reads() uint64 {
	return atomic.LoadUint64(&s.read)
}

// Writes returns the number of written bytes.
func (s *IOStats) Writes() uint64 {
	return atomic.LoadUint64(&s.write)
}
