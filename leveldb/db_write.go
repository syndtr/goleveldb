// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func (d *DB) doWriteJournal(b *Batch) error {
	w, err := d.journal.Next()
	if err != nil {
		return err
	}
	if _, err := w.Write(b.encode()); err != nil {
		return err
	}
	if err := d.journal.Flush(); err != nil {
		return err
	}
	if b.sync {
		return d.journalWriter.Sync()
	}
	return nil
}

func (d *DB) writeJournal() {
	defer d.closeWg.Done()
	for {
		select {
		case _, _ = <-d.closeCh:
			return
		case b := <-d.journalCh:
			if b != nil {
				d.journalAckCh <- d.doWriteJournal(b)
			}
		}
	}
}

func (d *DB) flush() (mem *memdb.DB, err error) {
	s := d.s

	delayed := false
	flush := func() bool {
		v := s.version()
		defer v.release()
		mem = d.getEffectiveMem()
		switch {
		case v.tLen(0) >= kL0_SlowdownWritesTrigger && !delayed:
			delayed = true
			time.Sleep(time.Millisecond)
		case mem.Size() < s.o.GetWriteBuffer():
			return false
		case v.tLen(0) >= kL0_StopWritesTrigger:
			delayed = true
			err = d.wakeCompaction(2)
			if err != nil {
				return false
			}
		default:
			// Wait for pending memdb compaction.
			select {
			case _, _ = <-d.closeCh:
				err = ErrClosed
				return false
			case <-d.compMemAckCh:
			case err = <-d.compErrCh:
				return false
			}
			// Create new memdb and journal.
			mem, err = d.newMem()
			if err != nil {
				return false
			}

			// Schedule memdb compaction.
			d.compMemCh <- nil
			return false
		}
		return true
	}
	start := time.Now()
	for flush() {
	}
	if delayed {
		s.logf("db@write delayed T·%v", time.Since(start))
	}
	return
}

// Write apply the given batch to the DB. The batch will be applied
// sequentially.
//
// It is safe to modify the contents of the arguments after Write returns.
func (d *DB) Write(b *Batch, wo *opt.WriteOptions) (err error) {
	err = d.ok()
	if err != nil || b == nil || b.len() == 0 {
		return
	}

	b.init(wo.GetSync())

	// The write happen synchronously.
	select {
	case _, _ = <-d.closeCh:
		return ErrClosed
	case d.writeCh <- b:
		return <-d.writeAckCh
	case d.writeLockCh <- struct{}{}:
	}

	merged := 0
	defer func() {
		<-d.writeLockCh
		for i := 0; i < merged; i++ {
			d.writeAckCh <- err
		}
	}()

	mem, err := d.flush()
	if err != nil {
		return
	}

	// Calculate maximum size of the batch.
	m := 1 << 20
	if x := b.size(); x <= 128<<10 {
		m = x + (128 << 10)
	}

	// Merge with other batch.
drain:
	for b.size() <= m && !b.sync {
		select {
		case nb := <-d.writeCh:
			b.append(nb)
			merged++
		default:
			break drain
		}
	}

	// Set batch first seq number relative from last seq.
	b.seq = d.seq + 1

	// Write journal concurrently if it is large enough.
	if b.size() >= (128 << 10) {
		// Push the write batch to the journal writer
		select {
		case _, _ = <-d.closeCh:
			err = ErrClosed
			return
		case d.journalCh <- b:
			// Write into memdb
			b.memReplay(mem)
		}
		// Wait for journal writer
		select {
		case _, _ = <-d.closeCh:
			err = ErrClosed
			return
		case err = <-d.journalAckCh:
			if err != nil {
				// Revert memdb if error detected
				b.revertMemReplay(mem)
				return
			}
		}
	} else {
		err = d.doWriteJournal(b)
		if err != nil {
			return
		}
		b.memReplay(mem)
	}

	// Set last seq number.
	d.addSeq(uint64(b.len()))
	return
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Put returns.
func (d *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	b := new(Batch)
	b.Put(key, value)
	return d.Write(b, wo)
}

// Delete deletes the value for the given key. It returns ErrNotFound if
// the DB does not contain the key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (d *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	b := new(Batch)
	b.Delete(key)
	return d.Write(b, wo)
}
