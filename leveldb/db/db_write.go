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

package db

import (
	"leveldb/memdb"
	"time"
)

func (d *DB) flush() (m *memdb.DB, err error) {
	s := d.s

	delayed := false
	for {
		v := s.version()
		mem := d.getMem()
		switch {
		case v.tLen(0) >= kL0_SlowdownWritesTrigger && !delayed:
			delayed = true
			time.Sleep(1000 * time.Microsecond)
			continue
		case mem.cur.Size() <= s.o.GetWriteBuffer():
			// still room
			return mem.cur, nil
		case mem.froze != nil:
			d.cch <- cWait
			continue
		case v.tLen(0) >= kL0_StopWritesTrigger:
			d.cch <- cSched
			continue
		}

		// create new memdb and log
		m, err = d.newMem()
		if err != nil {
			return
		}

		// schedule compaction
		select {
		case d.cch <- cSched:
		default:
		}
	}

	return
}

func (d *DB) write() {
	// register to the WaitGroup
	d.ewg.Add(1)

	lch := make(chan *Batch)
	lack := make(chan error)
	go func() {
		for b := range lch {
			if b == nil {
				close(lch)
				close(lack)
				return
			}

			// write log
			err := d.log.log.Append(b.encode())
			if err == nil && b.sync {
				err = d.log.writer.Sync()
			}

			lack <- err
		}
	}()

	for exit := false; !exit; {
		b := <-d.wch
		if b == nil && d.isClosed() {
			break
		}

		mem, err := d.flush()
		if err != nil {
			b.done(err)
			continue
		}

		// calculate max size of batch
		n := b.size()
		m := 1 << 20
		if n <= 128<<10 {
			m = n + (128 << 10)
		}

		// merge with other batch
		for done := false; !done && b.size() <= m && !b.sync; {
			select {
			case nb := <-d.wch:
				if b == nil {
					if d.isClosed() {
						exit = true
						done = true
					}
				} else {
					b.append(nb)
				}
			default:
				done = true
			}
		}

		// set batch first seq number relative from last seq
		b.seq = d.seq + 1

		// write log
		lch <- b

		// replay batch to memdb
		b.memReplay(mem)

		// wait for log
		err = <-lack
		if err != nil {
			b.revertMemReplay(mem)
			b.done(err)
			continue
		}

		// set last seq number
		d.addSeq(uint64(b.len()))

		// done
		b.done(nil)
	}

	lch <- nil
	close(d.wch)
	d.ewg.Done()
}
