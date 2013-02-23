// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"time"

	"leveldb/memdb"
	"leveldb/opt"
)

func (d *DB) doWriteJournal(b *Batch) error {
	err := d.journal.journal.Append(b.encode())
	if err == nil && b.sync {
		err = d.journal.writer.Sync()
	}
	return err
}

func (d *DB) writeJournal() {
	for b := range d.jch {
		if b == nil {
			break
		}

		// write journal
		d.jack <- d.doWriteJournal(b)
	}

	close(d.jch)
	close(d.jack)
	d.ewg.Done()
}

func (d *DB) flush() (m *memdb.DB, err error) {
	s := d.s

	delayed, cwait := false, false
	for {
		v := s.version()
		mem := d.getMem()
		switch {
		case v.tLen(0) >= kL0_SlowdownWritesTrigger && !delayed:
			delayed = true
			time.Sleep(time.Millisecond)
			continue
		case mem.cur.Size() <= s.o.GetWriteBuffer():
			// still room
			return mem.cur, nil
		case mem.froze != nil:
			if cwait {
				if err = d.geterr(); err != nil {
					return
				}
				d.cch <- cSched
			} else {
				cwait = true
				d.cch <- cWait
			}
			continue
		case v.tLen(0) >= kL0_StopWritesTrigger:
			d.cch <- cSched
			continue
		}

		// create new memdb and journal
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

// Write apply the specified batch to the database.
func (d *DB) Write(b *Batch, wo *opt.WriteOptions) (err error) {
	err = d.wok()
	if err != nil || b == nil || b.len() == 0 {
		return
	}

	b.init(wo.HasFlag(opt.WFSync))

	select {
	case d.wqueue <- b:
		return <-d.wack
	case d.wlock <- struct{}{}:
	}

	merged := 0
	defer func() {
		<-d.wlock
		for i := 0; i < merged; i++ {
			d.wack <- err
		}
	}()

	mem, err := d.flush()
	if err != nil {
		return
	}

	// calculate maximum size of the batch
	m := 1 << 20
	if x := b.size(); x <= 128<<10 {
		m = x + (128 << 10)
	}

	// merge with other batch
drain:
	for b.size() <= m && !b.sync {
		select {
		case nb := <-d.wqueue:
			b.append(nb)
			merged++
		default:
			break drain
		}
	}

	// set batch first seq number relative from last seq
	b.seq = d.seq + 1

	// write journal concurrently if it is large enough
	if b.size() >= (128 << 10) {
		d.jch <- b
		b.memReplay(mem)
		err = <-d.jack
		if err != nil {
			b.revertMemReplay(mem)
			return
		}
	} else {
		err = d.doWriteJournal(b)
		if err != nil {
			return
		}
		b.memReplay(mem)
	}

	// set last seq number
	d.addSeq(uint64(b.len()))

	return
}

// Put set the database entry for "key" to "value".
func (d *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	b := new(Batch)
	b.Put(key, value)
	return d.Write(b, wo)
}

// Delete remove the database entry (if any) for "key". It is not an error
// if "key" did not exist in the database.
func (d *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	b := new(Batch)
	b.Delete(key)
	return d.Write(b, wo)
}
