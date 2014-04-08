// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	ErrNotFound         = util.ErrNotFound
	ErrSnapshotReleased = errors.New("leveldb: snapshot released")
	ErrIterReleased     = errors.New("leveldb: iterator released")
	ErrClosed           = errors.New("leveldb: closed")
)

// DB is a LevelDB database.
type DB struct {
	// Need 64-bit alignment.
	seq uint64

	s *session

	// MemDB
	memMu             sync.RWMutex
	mem               *memdb.DB
	frozenMem         *memdb.DB
	journal           *journal.Writer
	journalWriter     storage.Writer
	journalFile       storage.File
	frozenJournalFile storage.File
	frozenSeq         uint64

	// Snapshot
	snapsMu   sync.Mutex
	snapsRoot snapshotElement

	// Write
	writeCh      chan *Batch
	writeLockCh  chan struct{}
	writeAckCh   chan error
	journalCh    chan *Batch
	journalAckCh chan error

	// Compaction
	compCh       chan chan<- struct{}
	compMemCh    chan chan<- struct{}
	compMemAckCh chan struct{}
	compReqCh    chan *cReq
	compErrCh    chan error
	compErrSetCh chan error
	compStats    [kNumLevels]cStats

	// Close
	closeWg sync.WaitGroup
	closeCh chan struct{}
	closed  uint32
	closer  io.Closer
}

func openDB(s *session) (*DB, error) {
	s.log("db@open opening")
	start := time.Now()
	db := &DB{
		s: s,
		// Initial sequence
		seq: s.stSeq,
		// Write
		writeCh:      make(chan *Batch),
		writeLockCh:  make(chan struct{}, 1),
		writeAckCh:   make(chan error),
		journalCh:    make(chan *Batch),
		journalAckCh: make(chan error),
		// Compaction
		compCh:       make(chan chan<- struct{}, 1),
		compMemCh:    make(chan chan<- struct{}, 1),
		compMemAckCh: make(chan struct{}, 1),
		compReqCh:    make(chan *cReq),
		compErrCh:    make(chan error),
		compErrSetCh: make(chan error),
		// Close
		closeCh: make(chan struct{}),
	}
	db.initSnapshot()
	db.compMemAckCh <- struct{}{}

	if err := db.recoverJournal(); err != nil {
		return nil, err
	}

	// Remove any obsolete files.
	if err := db.cleanFiles(); err != nil {
		return nil, err
	}

	// Don't include compaction error goroutine into wait group.
	go db.compactionError()

	db.closeWg.Add(2)
	go db.compaction()
	go db.writeJournal()
	db.wakeCompaction(0)

	s.logf("db@open done T·%v", time.Since(start))

	runtime.SetFinalizer(db, (*DB).Close)
	return db, nil
}

// Open opens or creates a DB for the given storage.
// The DB will be created if not exist, unless ErrorIfMissing is true.
// Also, if ErrorIfExist is true and the DB exist Open will returns
// os.ErrExist error.
//
// Open will return an error with type of ErrManifest if manifest file
// is missing or corrupted. Missing or corrupted manifest file can be
// recovered with Recover function.
//
// The DB must be closed after use, by calling Close method.
func Open(p storage.Storage, o *opt.Options) (db *DB, err error) {
	s, err := newSession(p, o)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			s.close()
			s.release()
		}
	}()

	err = s.recover()
	if err != nil {
		if !os.IsNotExist(err) || s.o.GetErrorIfMissing() {
			return
		}
		err = s.create()
		if err != nil {
			return
		}
	} else if s.o.GetErrorIfExist() {
		err = os.ErrExist
		return
	}

	return openDB(s)
}

// OpenFile opens or creates a DB for the given path.
// The DB will be created if not exist, unless ErrorIfMissing is true.
// Also, if ErrorIfExist is true and the DB exist OpenFile will returns
// os.ErrExist error.
//
// OpenFile uses standard file-system backed storage implementation as
// desribed in the leveldb/storage package.
//
// OpenFile will return an error with type of ErrManifest if manifest file
// is missing or corrupted. Missing or corrupted manifest file can be
// recovered with Recover function.
//
// The DB must be closed after use, by calling Close method.
func OpenFile(path string, o *opt.Options) (db *DB, err error) {
	stor, err := storage.OpenFile(path)
	if err != nil {
		return
	}
	db, err = Open(stor, o)
	if err != nil {
		stor.Close()
	} else {
		db.closer = stor
	}
	return
}

// Recover recovers and opens a DB with missing or corrupted manifest files
// for the given storage. It will ignore any manifest files, valid or not.
// The DB must already exist or it will returns an error.
// Also, Recover will ignore ErrorIfMissing and ErrorIfExist options.
//
// The DB must be closed after use, by calling Close method.
func Recover(p storage.Storage, o *opt.Options) (db *DB, err error) {
	s, err := newSession(p, o)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			s.close()
			s.release()
		}
	}()

	// get all files
	ff0, err := s.getFiles(storage.TypeAll)
	if err != nil {
		return
	}

	ff := files(ff0)
	ff.sort()

	s.logf("db@recovery F·%d", len(ff))

	rec := new(sessionRecord)

	// recover tables
	var nt *tFile
	for _, f := range ff {
		if f.Type() != storage.TypeTable {
			continue
		}

		var r storage.Reader
		r, err = f.Open()
		if err != nil {
			return
		}
		var size int64
		size, err = r.Seek(0, 2)
		r.Close()
		if err != nil {
			return
		}

		t := newTFile(f, uint64(size), nil, nil)
		iter := s.tops.newIterator(t, nil, nil)
		// min ikey
		if iter.First() {
			t.min = iter.Key()
		} else {
			err = iter.Error()
			iter.Release()
			if err != nil {
				return
			} else {
				continue
			}
		}
		// max ikey
		if iter.Last() {
			t.max = iter.Key()
		} else {
			err = iter.Error()
			iter.Release()
			if err != nil {
				return
			} else {
				continue
			}
		}
		iter.Release()
		s.logf("db@recovery found table @%d S·%s %q:%q", t.file.Num(), shortenb(int(t.size)), t.min, t.max)
		// add table to level 0
		rec.addTableFile(0, t)
		nt = t
	}

	// extract largest seq number from newest table
	if nt != nil {
		var lseq uint64
		iter := s.tops.newIterator(nt, nil, nil)
		for iter.Next() {
			seq, _, ok := iKey(iter.Key()).parseNum()
			if !ok {
				continue
			}
			if seq > lseq {
				lseq = seq
			}
		}
		iter.Release()
		rec.setSeq(lseq)
	}

	// set file num based on largest one
	if len(ff) > 0 {
		s.stFileNum = ff[len(ff)-1].Num() + 1
	} else {
		s.stFileNum = 0
	}

	// create brand new manifest
	err = s.create()
	if err != nil {
		return
	}
	// commit record
	err = s.commit(rec)
	if err != nil {
		return
	}
	return openDB(s)
}

// RecoverFile recovers and opens a DB with missing or corrupted manifest files
// for the given path. It will ignore any manifest files, valid or not.
// The DB must already exist or it will returns an error.
// Also, Recover will ignore ErrorIfMissing and ErrorIfExist options.
//
// RecoverFile uses standard file-system backed storage implementation as desribed
// in the leveldb/storage package.
//
// The DB must be closed after use, by calling Close method.
func RecoverFile(path string, o *opt.Options) (db *DB, err error) {
	stor, err := storage.OpenFile(path)
	if err != nil {
		return
	}
	db, err = Recover(stor, o)
	if err != nil {
		stor.Close()
	} else {
		db.closer = stor
	}
	return
}

func (d *DB) recoverJournal() error {
	s := d.s
	icmp := s.cmp

	ff0, err := s.getFiles(storage.TypeJournal)
	if err != nil {
		return err
	}
	ff1 := files(ff0)
	ff1.sort()
	ff2 := make([]storage.File, 0, len(ff1))
	for _, file := range ff1 {
		if file.Num() >= s.stJournalNum || file.Num() == s.stPrevJournalNum {
			s.markFileNum(file.Num())
			ff2 = append(ff2, file)
		}
	}

	var jr *journal.Reader
	var of storage.File
	var mem *memdb.DB
	batch := new(Batch)
	cm := newCMem(s)
	buf := new(util.Buffer)
	// Options.
	strict := s.o.GetStrict(opt.StrictJournal)
	checksum := s.o.GetStrict(opt.StrictJournalChecksum)
	writeBuffer := s.o.GetWriteBuffer()
	recoverJournal := func(file storage.File) error {
		s.logf("journal@recovery recovering @%d", file.Num())
		reader, err := file.Open()
		if err != nil {
			return err
		}
		defer reader.Close()
		if jr == nil {
			jr = journal.NewReader(reader, dropper{s, file}, strict, checksum)
		} else {
			jr.Reset(reader, dropper{s, file}, strict, checksum)
		}
		if of != nil {
			if mem.Len() > 0 {
				if err := cm.flush(mem, 0); err != nil {
					return err
				}
			}
			if err := cm.commit(file.Num(), d.seq); err != nil {
				return err
			}
			cm.reset()
			of.Remove()
			of = nil
		}
		// Reset memdb.
		mem.Reset()
		for {
			r, err := jr.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			buf.Reset()
			if _, err := buf.ReadFrom(r); err != nil {
				if strict {
					return err
				}
				continue
			}
			if err := batch.decode(buf.Bytes()); err != nil {
				return err
			}
			if err := batch.memReplay(mem); err != nil {
				return err
			}
			d.seq = batch.seq + uint64(batch.len())
			if mem.Size() >= writeBuffer {
				// Large enough, flush it.
				if err := cm.flush(mem, 0); err != nil {
					return err
				}
				// Reset memdb.
				mem.Reset()
			}
		}
		of = file
		return nil
	}
	// Recover all journals.
	if len(ff2) > 0 {
		s.logf("journal@recovery F·%d", len(ff2))
		mem = memdb.New(icmp, toPercent(writeBuffer, kWriteBufferPercent))
		for _, file := range ff2 {
			if err := recoverJournal(file); err != nil {
				return err
			}
		}
		// Flush the last journal.
		if mem.Len() > 0 {
			if err := cm.flush(mem, 0); err != nil {
				return err
			}
		}
	}
	// Create a new journal.
	if _, err := d.newMem(); err != nil {
		return err
	}
	// Commit.
	if err := cm.commit(d.journalFile.Num(), d.seq); err != nil {
		return err
	}
	// Remove the last journal.
	if of != nil {
		of.Remove()
	}
	return nil
}

func (d *DB) get(key []byte, seq uint64, ro *opt.ReadOptions) (value []byte, err error) {
	s := d.s

	ucmp := s.cmp.cmp
	ikey := newIKey(key, seq, tSeek)

	em, fm := d.getMems()
	for _, m := range [...]*memdb.DB{em, fm} {
		if m == nil {
			continue
		}
		mk, mv, me := m.Find(ikey)
		if me == nil {
			ukey, _, t, ok := parseIkey(mk)
			if ok && ucmp.Compare(ukey, key) == 0 {
				if t == tDel {
					return nil, ErrNotFound
				}
				return mv, nil
			}
		} else if me != ErrNotFound {
			return nil, me
		}
	}

	v := s.version()
	value, cSched, err := v.get(ikey, ro)
	v.release()
	if cSched {
		// Wake compaction.
		d.wakeCompaction(0)
	}
	return
}

// Get gets the value for the given key. It returns ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (d *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	err = d.ok()
	if err != nil {
		return
	}

	return d.get(key, d.getSeq(), ro)
}

// NewIterator returns an iterator for the latest snapshot of the
// uderlying DB.
// The returned iterator is not goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. The resultant key/value pairs are guaranteed to be
// consistent.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (d *DB) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	if err := d.ok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}

	p := d.newSnapshot()
	defer p.Release()
	return p.NewIterator(slice, ro)
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (d *DB) GetSnapshot() (*Snapshot, error) {
	if err := d.ok(); err != nil {
		return nil, err
	}

	return d.newSnapshot(), nil
}

// GetProperty returns value of the given property name.
//
// Property names:
//	leveldb.num-files-at-level{n}
//		Returns the number of filer at level 'n'.
//	leveldb.stats
//		Returns statistics of the underlying DB.
//	leveldb.sstables
//		Returns sstables list for each level.
func (d *DB) GetProperty(name string) (value string, err error) {
	err = d.ok()
	if err != nil {
		return
	}

	const prefix = "leveldb."
	if !strings.HasPrefix(name, prefix) {
		return "", errors.New("leveldb: GetProperty: unknown property: " + name)
	}

	p := name[len(prefix):]

	s := d.s
	v := s.version()
	defer v.release()

	switch {
	case strings.HasPrefix(p, "num-files-at-level"):
		var level uint
		var rest string
		n, _ := fmt.Scanf("%d%s", &level, &rest)
		if n != 1 || level >= kNumLevels {
			err = errors.New("leveldb: GetProperty: invalid property: " + name)
		} else {
			value = fmt.Sprint(v.tLen(int(level)))
		}
	case p == "stats":
		value = "Compactions\n" +
			" Level |   Tables   |    Size(MB)   |    Time(sec)  |    Read(MB)   |   Write(MB)\n" +
			"-------+------------+---------------+---------------+---------------+---------------\n"
		for level, tt := range v.tables {
			duration, read, write := d.compStats[level].get()
			if len(tt) == 0 && duration == 0 {
				continue
			}
			value += fmt.Sprintf(" %3d   | %10d | %13.5f | %13.5f | %13.5f | %13.5f\n",
				level, len(tt), float64(tt.size())/1048576.0, duration.Seconds(),
				float64(read)/1048576.0, float64(write)/1048576.0)
		}
	case p == "sstables":
		for level, tt := range v.tables {
			value += fmt.Sprintf("--- level %d ---\n", level)
			for _, t := range tt {
				value += fmt.Sprintf("%d:%d[%q .. %q]\n", t.file.Num(), t.size, t.min, t.max)
			}
		}
	default:
		err = errors.New("leveldb: GetProperty: unknown property: " + name)
	}

	return
}

// GetApproximateSizes calculates approximate sizes of the given key ranges.
// The length of the returned sizes are equal with the length of the given
// ranges. The returned sizes measure storage space usage, so if the user
// data compresses by a factor of ten, the returned sizes will be one-tenth
// the size of the corresponding user data size.
// The results may not include the sizes of recently written data.
func (d *DB) GetApproximateSizes(ranges []util.Range) (Sizes, error) {
	if err := d.ok(); err != nil {
		return nil, err
	}

	v := d.s.version()
	defer v.release()

	sizes := make(Sizes, 0, len(ranges))
	for _, r := range ranges {
		min := newIKey(r.Start, kMaxSeq, tSeek)
		max := newIKey(r.Limit, kMaxSeq, tSeek)
		start, err := v.getApproximateOffset(min)
		if err != nil {
			return nil, err
		}
		limit, err := v.getApproximateOffset(max)
		if err != nil {
			return nil, err
		}
		var size uint64
		if limit >= start {
			size = limit - start
		}
		sizes = append(sizes, size)
	}

	return sizes, nil
}

// CompactRange compacts the underlying DB for the given key range.
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data. This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
func (d *DB) CompactRange(r util.Range) error {
	err := d.ok()
	if err != nil {
		return err
	}

	cch := make(chan struct{})
	req := &cReq{
		level: -1,
		min:   r.Start,
		max:   r.Limit,
		cch:   cch,
	}

	// Push manual compaction request.
	select {
	case _, _ = <-d.closeCh:
		return ErrClosed
	case err := <-d.compErrCh:
		return err
	case d.compReqCh <- req:
	}
	// Wait for compaction
	select {
	case _, _ = <-d.closeCh:
		return ErrClosed
	case <-cch:
	}
	return nil
}

// Close closes the DB. This will also releases any outstanding snapshot.
//
// It is not safe to close a DB until all outstanding iterators are released.
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (d *DB) Close() error {
	if !d.setClosed() {
		return ErrClosed
	}

	s := d.s
	start := time.Now()
	s.log("db@close closing")

	// Clear the finalizer.
	runtime.SetFinalizer(d, nil)

	// Get compaction error.
	var err error
	select {
	case err = <-d.compErrCh:
	default:
	}

	close(d.closeCh)

	// wait for the WaitGroup
	d.closeWg.Wait()

	// close journal
	if d.journal != nil {
		d.journal.Close()
		d.journalWriter.Close()
	}

	// close session
	s.close()
	s.logf("db@close done T·%v", time.Since(start))
	s.release()

	if d.closer != nil {
		if err1 := d.closer.Close(); err == nil {
			err = err1
		}
	}

	return err
}
