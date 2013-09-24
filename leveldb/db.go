// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// DB is a LevelDB database.
type DB struct {
	// Need 64-bit alignment.
	seq uint64

	s *session

	cch     chan cSignal       // compaction worker signal
	creq    chan *cReq         // compaction request
	wlock   chan struct{}      // writer mutex
	wqueue  chan *Batch        // writer queue
	wack    chan error         // writer ack
	jch     chan *Batch        // journal writer chan
	jack    chan error         // journal writer ack
	ewg     sync.WaitGroup     // exit WaitGroup
	cstats  [kNumLevels]cStats // Compaction stats
	closeCb func() error
	closed  uint32
	err     unsafe.Pointer

	// MemDB
	memMu         sync.RWMutex
	mem           *memdb.DB
	frozenMem     *memdb.DB
	journal       *journalWriter
	frozenJournal *journalWriter
	frozenSeq     uint64

	// Snapshot
	snapsMu   sync.Mutex
	snapsRoot snapshotElement
}

func openDB(s *session) (*DB, error) {
	db := &DB{
		s:      s,
		cch:    make(chan cSignal),
		creq:   make(chan *cReq),
		wlock:  make(chan struct{}, 1),
		wqueue: make(chan *Batch),
		wack:   make(chan error),
		jch:    make(chan *Batch),
		jack:   make(chan error),
		seq:    s.stSeq,
	}
	db.initSnapshot()

	if err := db.recoverJournal(); err != nil {
		return nil, err
	}

	// remove any obsolete files
	db.cleanFiles()

	db.ewg.Add(2)
	go db.compaction()
	go db.writeJournal()
	// wait for compaction goroutine
	db.cch <- cWait

	runtime.SetFinalizer(db, (*DB).Close)
	return db, nil
}

// Open opens or creates a DB for the given storage.
// If opt.OFCreateIfMissing is set then the DB will be created if not exist,
// otherwise it will returns an error. If opt.OFErrorIfExist is set and the DB
// exist Open will returns os.ErrExist error.
//
// The DB must be closed after use, by calling Close method.
func Open(p storage.Storage, o *opt.Options) (*DB, error) {
	s, err := openSession(p, o)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	err = s.recover()
	if os.IsNotExist(err) && s.o.HasFlag(opt.OFCreateIfMissing) {
		err = s.create()
	} else if err == nil && s.o.HasFlag(opt.OFErrorIfExist) {
		err = os.ErrExist
	}
	if err != nil {
		return nil, err
	}

	return openDB(s)
}

// Open opens or creates a DB for the given path. OpenFile uses standard
// file-system backed storage implementation as desribed in the
// leveldb/storage package.
// If opt.OFCreateIfMissing is set then the DB will be created if not exist,
// otherwise it will returns error. If opt.OFErrorIfExist is set and the DB
// exist OpenFile will returns os.ErrExist error.
//
// The DB must be closed after use, by calling Close method.
func OpenFile(path string, o *opt.Options) (*DB, error) {
	stor, err := storage.OpenFile(path)
	if err != nil {
		return nil, err
	}
	db, err := Open(stor, o)
	db.closeCb = func() error {
		return stor.Close()
	}
	return db, err
}

// Recover recovers and opens a DB with missing or corrupted manifest files
// for the given storage. It will ignore any manifest files, valid or not.
// The DB must already exist or it will returns an error.
// Also Recover will ignore opt.OFCreateIfMissing and opt.OFErrorIfExist flags.
//
// The DB must be closed after use, by calling Close method.
func Recover(p storage.Storage, o *opt.Options) (*DB, error) {
	s, err := openSession(p, o)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	// get all files
	ff := files(s.getFiles(storage.TypeAll))
	ff.sort()

	s.printf("Recover: started, files=%d", len(ff))

	rec := new(sessionRecord)

	// recover tables
	ro := &opt.ReadOptions{}
	var nt *tFile
	for _, f := range ff {
		if f.Type() != storage.TypeTable {
			continue
		}

		var size uint64
		size, err = f.Size()
		if err != nil {
			return nil, err
		}

		t := newTFile(f, size, nil, nil)
		iter := s.tops.newIterator(t, ro)
		// min ikey
		if iter.First() {
			t.min = iter.Key()
		} else if err := iter.Error(); err != nil {
			iter.Release()
			return nil, err
		} else {
			iter.Release()
			continue
		}
		// max ikey
		if iter.Last() {
			t.max = iter.Key()
		} else if err := iter.Error(); err != nil {
			iter.Release()
			return nil, err
		} else {
			iter.Release()
			continue
		}
		iter.Release()

		// add table to level 0
		rec.addTableFile(0, t)

		nt = t
	}

	// extract largest seq number from newest table
	if nt != nil {
		var lseq uint64
		iter := s.tops.newIterator(nt, ro)
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
	s.stFileNum = ff[len(ff)-1].Num() + 1

	// create brand new manifest
	if err = s.create(); err != nil {
		return nil, err
	}
	// commit record
	if err = s.commit(rec); err != nil {
		return nil, err
	}

	return openDB(s)
}

func (d *DB) recoverJournal() error {
	s := d.s
	icmp := s.cmp

	s.printf("JournalRecovery: started, min=%d", s.stJournalNum)

	var mem *memdb.DB
	batch := new(Batch)
	cm := newCMem(s)

	journals := files(s.getFiles(storage.TypeJournal))
	journals.sort()
	rJournals := make([]storage.File, 0, len(journals))
	for _, journal := range journals {
		if journal.Num() >= s.stJournalNum || journal.Num() == s.stPrevJournalNum {
			s.markFileNum(journal.Num())
			rJournals = append(rJournals, journal)
		}
	}

	var fr *journalReader
	for _, journal := range rJournals {
		s.printf("JournalRecovery: recovering, num=%d", journal.Num())

		r, err := newJournalReader(journal, true, s.journalDropFunc("journal", journal.Num()))
		if err != nil {
			return err
		}

		if mem != nil {
			if mem.Len() > 0 {
				if err = cm.flush(mem, 0); err != nil {
					return err
				}
			}

			if err = cm.commit(r.file.Num(), d.seq); err != nil {
				return err
			}

			cm.reset()

			fr.remove()
			fr = nil
		}

		mem = memdb.New(icmp, toPercent(s.o.GetWriteBuffer(), kWriteBufferPercent))

		for r.journal.Next() {
			if err = batch.decode(r.journal.Record()); err != nil {
				return err
			}

			if err = batch.memReplay(mem); err != nil {
				return err
			}

			d.seq = batch.seq + uint64(batch.len())

			if mem.Size() >= s.o.GetWriteBuffer() {
				// flush to table
				if err = cm.flush(mem, 0); err != nil {
					return err
				}

				// create new memdb
				mem = memdb.New(icmp, toPercent(s.o.GetWriteBuffer(), kWriteBufferPercent))
			}
		}

		if err = r.journal.Error(); err != nil {
			return err
		}

		r.close()
		fr = r
	}

	// create new journal
	if _, err := d.newMem(); err != nil {
		return err
	}

	if mem != nil && mem.Len() > 0 {
		if err := cm.flush(mem, 0); err != nil {
			return err
		}
	}

	if err := cm.commit(d.journal.file.Num(), d.seq); err != nil {
		return err
	}

	if fr != nil {
		fr.remove()
	}

	return nil
}

// GetOptionsSetter returns and opt.OptionsSetter for the DB.
// The opt.OptionsSetter allows modify options of an opened DB safely,
// as documented in the leveldb/opt package.
func (d *DB) GetOptionsSetter() opt.OptionsSetter {
	return d.s.o
}

func (d *DB) get(key []byte, seq uint64, ro *opt.ReadOptions) (value []byte, err error) {
	s := d.s

	ucmp := s.cmp.cmp
	ikey := newIKey(key, seq, tSeek)

	memGet := func(m *memdb.DB) bool {
		var rkey []byte
		rkey, value, err = m.Find(ikey)
		if err != nil {
			return false
		}
		ukey, _, t, ok := parseIkey(rkey)
		if !ok || ucmp.Compare(ukey, key) != 0 || t == tDel {
			value = nil
			err = errors.ErrNotFound
			return false
		}
		return true
	}

	if mem, frozenMem := d.getMem(); memGet(mem) || (frozenMem != nil && memGet(frozenMem)) {
		return
	}

	v := s.version()
	value, cState, err := v.get(ikey, ro)
	v.release()
	if cState && !d.isClosed() {
		// schedule compaction
		select {
		case d.cch <- cSched:
		default:
		}
	}
	return
}

// Get gets the value for the given key. It returns error.ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (d *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	err = d.rok()
	if err != nil {
		return
	}

	value, err = d.get(key, d.getSeq(), ro)
	if !ro.HasFlag(opt.RFDontCopyBuffer) {
		value = append([]byte{}, value...)
	}
	return
}

// NewIterator returns an iterator for the latest snapshot of the
// uderlying DB.
// The returned iterator is not goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. The resultant key/value pairs are guaranteed to be
// consistent.
//
// The iterator must be released after use, by calling Release method.
func (d *DB) NewIterator(ro *opt.ReadOptions) iterator.Iterator {
	if err := d.rok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}

	p := d.newSnapshot()
	defer p.Release()
	return p.NewIterator(ro)
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (d *DB) GetSnapshot() (*Snapshot, error) {
	if err := d.rok(); err != nil {
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
	err = d.rok()
	if err != nil {
		return
	}

	const prefix = "leveldb."
	if !strings.HasPrefix(name, prefix) {
		return "", errors.ErrInvalid("unknown property: " + name)
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
			err = errors.ErrInvalid("invalid property: " + name)
		} else {
			value = fmt.Sprint(v.tLen(int(level)))
		}
	case p == "stats":
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
		for level, tt := range v.tables {
			value += fmt.Sprintf("--- level %d ---\n", level)
			for _, t := range tt {
				value += fmt.Sprintf("%d:%d[%q .. %q]\n", t.file.Num(), t.size, t.min, t.max)
			}
		}
	default:
		err = errors.ErrInvalid("unknown property: " + name)
	}

	return
}

// GetApproximateSizes calculates approximate sizes of the given key ranges.
// The length of the returned sizes are equal with the length of the given
// ranges. The returned sizes measure storage space usage, so if the user
// data compresses by a factor of ten, the returned sizes will be one-tenth
// the size of the corresponding user data size.
// The results may not include the sizes of recently written data.
func (d *DB) GetApproximateSizes(ranges []Range) (Sizes, error) {
	if err := d.rok(); err != nil {
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
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
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

// Close closes the DB. This will also releases any outstanding snapshot.
//
// It is not safe to close a DB until all outstanding iterators are released.
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (d *DB) Close() error {
	if !d.setClosed() {
		return errors.ErrClosed
	}

	// Clear the finalizer.
	runtime.SetFinalizer(d, nil)

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

	// wake journal writer goroutine
	d.jch <- nil

	// wake Compaction goroutine
	d.cch <- cClose

	// wait for the WaitGroup
	d.ewg.Wait()

	// close journal
	if d.journal != nil {
		d.journal.close()
	}

	// close session
	d.s.close()

	if d.closeCb != nil {
		cerr := d.closeCb()
		if err := d.geterr(); err != nil {
			return err
		}
		return cerr
	}

	return d.geterr()
}
