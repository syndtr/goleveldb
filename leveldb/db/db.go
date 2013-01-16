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

// Package db provide implementation of LevelDB database.
package db

import (
	"fmt"
	"leveldb/descriptor"
	"leveldb/errors"
	"leveldb/iter"
	"leveldb/memdb"
	"leveldb/opt"
	"os"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

// DB represent a database session.
type DB struct {
	s *session

	cch    chan cSignal       // compaction worker signal
	creq   chan *cReq         // compaction request
	wlock  chan struct{}      // writer mutex
	wqueue chan *Batch        // writer queue
	wack   chan error         // writer ack
	lch    chan *Batch        // log writer chan
	lack   chan error         // log writer ack
	ewg    sync.WaitGroup     // exit WaitGroup
	cstats [kNumLevels]cStats // Compaction stats

	mem       unsafe.Pointer
	log, flog *logWriter
	seq, fseq uint64
	snaps     *snaps
	closed    uint32
	err       unsafe.Pointer
}

// Open open or create database from given desc.
func Open(desc descriptor.Descriptor, o *opt.Options) (d *DB, err error) {
	s := newSession(desc, o)

	err = s.recover()
	if os.IsNotExist(err) && o.HasFlag(opt.OFCreateIfMissing) {
		err = s.create()
	} else if err == nil && o.HasFlag(opt.OFErrorIfExist) {
		err = os.ErrExist
	}
	if err != nil {
		return
	}

	d = &DB{
		s:      s,
		cch:    make(chan cSignal),
		creq:   make(chan *cReq),
		wlock:  make(chan struct{}, 1),
		wqueue: make(chan *Batch),
		wack:   make(chan error),
		lch:    make(chan *Batch),
		lack:   make(chan error),
		seq:    s.stSeq,
		snaps:  newSnaps(),
	}

	err = d.recoverLog()
	if err != nil {
		return
	}

	// remove any obsolete files
	d.cleanFiles()

	go d.compaction()
	go d.writeLog()
	// wait for compaction goroutine
	d.cch <- cWait

	return
}

func (d *DB) recoverLog() (err error) {
	s := d.s
	icmp := s.cmp

	s.printf("LogRecovery: started, min=%d", s.stLogNum)

	mb := new(memBatch)
	cm := newCMem(s)

	logs, skip := files(s.getFiles(descriptor.TypeLog)), 0
	logs.sort()
	for _, log := range logs {
		if log.Number() < s.stLogNum {
			skip++
			continue
		}
		s.markFileNum(log.Number())
	}

	var r, fr *logReader
	for _, log := range logs[skip:] {
		s.printf("LogRecovery: recovering, num=%d", log.Number())

		r, err = newLogReader(log, true)
		if err != nil {
			return
		}

		if mb.mem != nil {
			if mb.mem.Len() > 0 {
				err = cm.flush(mb.mem, 0)
				if err != nil {
					return
				}
			}

			err = cm.commit(r.file.Number(), d.seq)
			if err != nil {
				return
			}

			cm.reset()

			fr.remove()
			fr = nil
		}

		mb.mem = memdb.New(icmp)

		for r.log.Next() {
			d.seq, err = replayBatch(r.log.Record(), mb)
			if err != nil {
				return
			}

			if mb.mem.Size() > s.o.GetWriteBuffer() {
				// flush to table
				err = cm.flush(mb.mem, 0)
				if err != nil {
					return
				}

				// create new memdb
				mb.mem = memdb.New(icmp)
			}
		}

		err = r.log.Error()
		if err != nil {
			return
		}

		r.close()
		fr = r
	}

	// create new log
	_, err = d.newMem()
	if err != nil {
		return
	}

	if mb.mem != nil && mb.mem.Len() > 0 {
		err = cm.flush(mb.mem, 0)
		if err != nil {
			return
		}
	}

	err = cm.commit(d.log.file.Number(), d.seq)
	if err != nil {
		return
	}

	if fr != nil {
		fr.remove()
	}

	return
}

func (d *DB) get(key []byte, seq uint64, ro *opt.ReadOptions) (value []byte, err error) {
	s := d.s

	ucmp := s.cmp.cmp
	ikey := newIKey(key, seq, tSeek)

	memGet := func(m *memdb.DB) bool {
		var k []byte
		k, value, err = m.Get(ikey)
		if err != nil {
			return false
		}
		ik := iKey(k)
		if ucmp.Compare(ik.ukey(), key) != 0 {
			return false
		}
		if _, t, ok := ik.parseNum(); ok {
			if t == tDel {
				value = nil
				err = errors.ErrNotFound
			}
			return true
		}
		return false
	}

	mem := d.getMem()
	if memGet(mem.cur) || (mem.froze != nil && memGet(mem.froze)) {
		return
	}

	value, cState, err := s.version().get(ikey, ro)

	if cState && !d.isClosed() {
		// schedule compaction
		select {
		case d.cch <- cSched:
		default:
		}
	}

	return
}

// Get get value for given key of the latest snapshot of database.
func (d *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	err = d.rok()
	if err != nil {
		return
	}

	return d.get(key, d.getSeq(), ro)
}

// NewIterator return an iterator over the contents of the latest snapshot of
// database. The result of NewIterator() is initially invalid (caller must
// call Next or one of Seek method, ie First, Last or Seek).
func (d *DB) NewIterator(ro *opt.ReadOptions) iter.Iterator {
	p := d.newSnapshot()
	i := p.NewIterator(ro)
	x, ok := i.(*Iterator)
	if ok {
		runtime.SetFinalizer(x, func(x *Iterator) {
			p.Release()
		})
	} else {
		p.Release()
	}
	return i
}

// GetSnapshot return a handle to the current DB state.
// Iterators created with this handle will all observe a stable snapshot
// of the current DB state. The caller must call *Snapshot.Release() when the
// snapshot is no longer needed.
func (d *DB) GetSnapshot() (snap *Snapshot, err error) {
	err = d.rok()
	if err != nil {
		return
	}

	snap = d.newSnapshot()
	runtime.SetFinalizer(snap, func(x *Snapshot) {
		x.Release()
	})
	return
}

// GetProperty used to query exported database state.
//
// Valid property names include:
//
//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
//     where <N> is an ASCII representation of a level number (e.g. "0").
//  "leveldb.stats" - returns a multi-line string that describes statistics
//     about the internal operation of the DB.
//  "leveldb.sstables" - returns a multi-line string that describes all
//     of the sstables that make up the db contents.
func (d *DB) GetProperty(prop string) (value string, err error) {
	err = d.rok()
	if err != nil {
		return
	}

	const prefix = "leveldb."
	if !strings.HasPrefix(prop, prefix) {
		return "", errors.ErrInvalid("unknown property: " + prop)
	}

	p := prop[len(prefix):]

	switch s := d.s; true {
	case strings.HasPrefix(p, "num-files-at-level"):
		var level uint
		var rest string
		n, _ := fmt.Scanf("%d%s", &level, &rest)
		if n != 1 || level >= kNumLevels {
			return "", errors.ErrInvalid("invalid property: " + prop)
		}
		value = fmt.Sprint(s.version().tLen(int(level)))
	case p == "stats":
		v := s.version()
		value = "Compactions\n" +
			" Level |   Tables   |    Size(MB)   |    Time(sec)  |    Read(MB)   |   Write(MB)\n" +
			"-------+------------+---------------+---------------+---------------+---------------\n"
		for level, tt := range v.tables {
			duration, read, write := d.cstats[level].get()
			if len(tt) == 0 && duration == 0 {
				continue
			}
			value += fmt.Sprintf(" %3d   | %10d | %13.5f | %13.5f | %13.5f | %13.5f\n",
				level, len(tt), float64(tt.size())/1048576.0, duration.Seconds(),
				float64(read)/1048576.0, float64(write)/1048576.0)
		}
	case p == "sstables":
		v := s.version()
		for level, tt := range v.tables {
			value += fmt.Sprintf("--- level %d ---\n", level)
			for _, t := range tt {
				value += fmt.Sprintf("%d:%d[%q .. %q]\n", t.file.Number(), t.size, t.min, t.max)
			}
		}
	default:
		return "", errors.ErrInvalid("unknown property: " + prop)
	}

	return
}

// GetApproximateSizes calculate approximate sizes of given ranges.
//
// Note that the returned sizes measure file system space usage, so
// if the user data compresses by a factor of ten, the returned
// sizes will be one-tenth the size of the corresponding user data size.
//
// The results may not include the sizes of recently written data.
func (d *DB) GetApproximateSizes(rr []Range) (sizes Sizes, err error) {
	err = d.rok()
	if err != nil {
		return
	}

	v := d.s.version()
	sizes = make(Sizes, 0, len(rr))
	for _, r := range rr {
		min := newIKey(r.Start, kMaxSeq, tSeek)
		max := newIKey(r.Limit, kMaxSeq, tSeek)
		start, err := v.approximateOffsetOf(min)
		if err != nil {
			return nil, err
		}
		limit, err := v.approximateOffsetOf(max)
		if err != nil {
			return nil, err
		}
		var size uint64
		if limit >= start {
			size = limit - start
		}
		sizes = append(sizes, size)
	}

	return
}

// CompactRange compact the underlying storage for the key range.
//
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data.  This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// Range.Start==nil is treated as a key before all keys in the database.
// Range.Limit==nil is treated as a key after all keys in the database.
// Therefore calling with Start==nil and Limit==nil will compact entire
// database.
func (d *DB) CompactRange(r Range) error {
	err := d.wok()
	if err != nil {
		return err
	}

	req := &cReq{level: -1}
	req.min = r.Start
	req.max = r.Limit

	d.creq <- req
	d.cch <- cWait

	return d.wok()
}

// Close closes the database. Snapshot and iterator are invalid
// after this call
func (d *DB) Close() error {
	if !d.setClosed() {
		return errors.ErrClosed
	}

	d.wlock <- struct{}{}
drain:
	for {
		select {
		case <-d.wqueue:
			d.wack <- errors.ErrClosed
		default:
			break drain
		}
	}
	close(d.wlock)

	// wake log writer goroutine
	d.lch <- nil

	// wake Compaction goroutine
	d.cch <- cClose

	// wait for the WaitGroup
	d.ewg.Wait()

	d.s.tops.purgeCache()
	cache := d.s.o.GetBlockCache()
	if cache != nil {
		cache.Purge(nil)
	}

	if d.log != nil {
		d.log.close()
	}
	if d.s.manifest != nil {
		d.s.manifest.close()
	}

	runtime.GC()

	return d.geterr()
}
