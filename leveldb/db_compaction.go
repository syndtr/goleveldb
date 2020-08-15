// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errCompactionTransactExiting = errors.New("leveldb: compaction transact exiting")
)

type cStat struct {
	duration time.Duration
	read     int64
	write    int64
}

func (p *cStat) add(n *cStatStaging) {
	p.duration += n.duration
	p.read += n.read
	p.write += n.write
}

func (p *cStat) get() (duration time.Duration, read, write int64) {
	return p.duration, p.read, p.write
}

type cStatStaging struct {
	start    time.Time
	duration time.Duration
	on       bool
	read     int64
	write    int64
}

func (p *cStatStaging) startTimer() {
	if !p.on {
		p.start = time.Now()
		p.on = true
	}
}

func (p *cStatStaging) stopTimer() {
	if p.on {
		p.duration += time.Since(p.start)
		p.on = false
	}
}

type cStats struct {
	lk    sync.Mutex
	stats []cStat
}

func (p *cStats) addStat(level int, n *cStatStaging) {
	p.lk.Lock()
	if level >= len(p.stats) {
		newStats := make([]cStat, level+1)
		copy(newStats, p.stats)
		p.stats = newStats
	}
	p.stats[level].add(n)
	p.lk.Unlock()
}

func (p *cStats) getStat(level int) (duration time.Duration, read, write int64) {
	p.lk.Lock()
	defer p.lk.Unlock()
	if level < len(p.stats) {
		return p.stats[level].get()
	}
	return
}

func (db *DB) compactionError() {
	var err error
noerr:
	// No error.
	for {
		select {
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
				goto haserr
			}
		case <-db.closeC:
			return
		}
	}
haserr:
	// Transient error.
	for {
		select {
		case db.compErrC <- err:
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
				goto noerr
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
			}
		case <-db.closeC:
			return
		}
	}
hasperr:
	// Persistent error.
	for {
		select {
		case db.compErrC <- err:
		case db.compPerErrC <- err:
		case db.writeLockC <- struct{}{}:
			// Hold write lock, so that write won't pass-through.
			db.compWriteLocking = true
		case <-db.closeC:
			if db.compWriteLocking {
				// We should release the lock or Close will hang.
				<-db.writeLockC
			}
			return
		}
	}
}

type compactionTransactCounter int

func (cnt *compactionTransactCounter) incr() {
	*cnt++
}

type compactionTransactInterface interface {
	run(cnt *compactionTransactCounter) error
	revert() error
}

func (db *DB) compactionTransact(name string, t compactionTransactInterface) {
	defer func() {
		if x := recover(); x != nil {
			if x == errCompactionTransactExiting {
				if err := t.revert(); err != nil {
					db.logf("%s revert error %q", name, err)
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)
	var (
		backoff  = backoffMin
		backoffT *time.Timer
		lastCnt  = compactionTransactCounter(0)

		disableBackoff = db.s.o.GetDisableCompactionBackoff()
	)
	defer func() {
		if backoffT != nil {
			backoffT.Stop()
		}
	}()
	for n := 0; ; n++ {
		// Check whether the DB is closed.
		if db.isClosed() {
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			db.logf("%s retrying N·%d", name, n)
		}

		// Execute.
		cnt := compactionTransactCounter(0)
		err := t.run(&cnt)
		if err != nil {
			db.logf("%s error I·%d %q", name, cnt, err)
		}

		// Set compaction error status.
		select {
		case db.compErrSetC <- err:
		case perr := <-db.compPerErrC:
			if err != nil {
				db.logf("%s exiting (persistent error %q)", name, perr)
				db.compactionExitTransact()
			}
		case <-db.closeC:
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		}
		if err == nil {
			return
		}
		if errors.IsCorrupted(err) {
			db.logf("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}

		if !disableBackoff {
			// Reset backoff duration if counter is advancing.
			if cnt > lastCnt {
				backoff = backoffMin
				lastCnt = cnt
			}

			// Backoff.
			if backoffT == nil {
				backoffT = time.NewTimer(backoff)
			} else {
				backoffT.Reset(backoff)
			}
			if backoff < backoffMax {
				backoff *= backoffMul
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			select {
			case <-backoffT.C:
			case <-db.closeC:
				db.logf("%s exiting", name)
				db.compactionExitTransact()
			}
		}
	}
}

type compactionTransactFunc struct {
	runFunc    func(cnt *compactionTransactCounter) error
	revertFunc func() error
}

func (t *compactionTransactFunc) run(cnt *compactionTransactCounter) error {
	return t.runFunc(cnt)
}

func (t *compactionTransactFunc) revert() error {
	if t.revertFunc != nil {
		return t.revertFunc()
	}
	return nil
}

func (db *DB) compactionTransactFunc(name string, run func(cnt *compactionTransactCounter) error, revert func() error) {
	db.compactionTransact(name, &compactionTransactFunc{run, revert})
}

func (db *DB) compactionExitTransact() {
	panic(errCompactionTransactExiting)
}

func (db *DB) compactionCommit(name string, rec *sessionRecord) {
	db.compCommitLk.Lock()
	defer db.compCommitLk.Unlock() // Defer is necessary.
	db.compactionTransactFunc(name+"@commit", func(cnt *compactionTransactCounter) error {
		return db.s.commit(rec, true)
	}, nil)
}

func (db *DB) pauseTableCompaction() {
	var resumeCh <-chan struct{}
noPause:
	for {
		select {
		case resumeCh = <-db.tcompPauseSetC:
			goto hasPause
		case <-db.closeC:
			return
		}
	}
hasPause:
	for {
		select {
		case ch := <-db.tcompPauseSetC:
			if ch != nil {
				panic("invalid resume channel")
			}
			resumeCh = nil
			goto noPause
		case db.tcompPauseC <- resumeCh:
		case <-db.closeC:
			return
		}
	}
}

func (db *DB) memCompaction() {
	mdb := db.getFrozenMem()
	if mdb == nil {
		return
	}
	defer mdb.decref()

	db.logf("memdb@flush N·%d S·%s", mdb.Len(), shortenb(mdb.Size()))

	// Don't compact empty memdb.
	if mdb.Len() == 0 {
		db.logf("memdb@flush skipping")
		// drop frozen memdb
		db.dropFrozenMem()
		return
	}

	// Pause table compaction.
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseSetC <- (<-chan struct{})(resumeC):
	case <-db.closeC:
		db.compactionExitTransact()
	}

	var (
		rec        = &sessionRecord{}
		stats      = &cStatStaging{}
		flushLevel int
	)

	// Generate tables.
	db.compactionTransactFunc("memdb@flush", func(cnt *compactionTransactCounter) (err error) {
		stats.startTimer()
		flushLevel, err = db.s.flushMemdb(rec, mdb.DB, db.memdbMaxLevel)
		stats.stopTimer()
		return
	}, func() error {
		for _, r := range rec.addedTables {
			db.logf("memdb@flush revert @%d", r.num)
			if err := db.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: r.num}); err != nil {
				return err
			}
		}
		return nil
	})

	rec.setJournalNum(db.journalFd.Num)
	rec.setSeqNum(db.frozenSeq)

	// Commit.
	stats.startTimer()
	db.compactionCommit("memdb", rec)
	stats.stopTimer()

	db.logf("memdb@flush committed F·%d T·%v", len(rec.addedTables), stats.duration)

	// Save compaction stats
	for _, r := range rec.addedTables {
		stats.write += r.size
	}
	db.compStats.addStat(flushLevel, stats)
	atomic.AddUint32(&db.memComp, 1)

	// Drop frozen memdb.
	db.dropFrozenMem()

	// Resume table compaction.
	select {
	case db.tcompPauseSetC <- nil:
	case <-db.closeC:
		db.compactionExitTransact()
	}
	close(resumeC)
	resumeC = nil

	// Trigger table compaction.
	db.compTrigger(db.tcompCmdC)
}

type tableCompactionBuilder struct {
	db           *DB
	s            *session
	c            *compaction
	rec          *sessionRecord
	stat0, stat1 *cStatStaging

	snapHasLastUkey bool
	snapLastUkey    []byte
	snapLastSeq     uint64
	snapIter        int
	snapKerrCnt     int
	snapDropCnt     int

	kerrCnt int
	dropCnt int

	minSeq    uint64
	strict    bool
	tableSize int

	tw *tWriter
}

func (b *tableCompactionBuilder) appendKV(key, value []byte) error {
	// Create new table if not already.
	if b.tw == nil {
		// Check for pause event.
		//if b.db != nil {
		//	select {
		//	case ch := <-b.db.tcompPauseC:
		//		b.db.pauseCompaction(ch)
		//	case <-b.db.closeC:
		//		b.db.compactionExitTransact()
		//	default:
		//	}
		//}

		// Create new table.
		var err error
		b.tw, err = b.s.tops.create()
		if err != nil {
			return err
		}
	}

	// Write key/value into table.
	return b.tw.append(key, value)
}

func (b *tableCompactionBuilder) needFlush() bool {
	return b.tw.tw.BytesLen() >= b.tableSize
}

func (b *tableCompactionBuilder) flush() error {
	t, err := b.tw.finish()
	if err != nil {
		return err
	}
	b.rec.addTableFile(b.c.sourceLevel+1, t)
	b.stat1.write += t.size
	b.s.logf("table@build created L%d@%d N·%d S·%s %q:%q", b.c.sourceLevel+1, t.fd.Num, b.tw.tw.EntriesLen(), shortenb(int(t.size)), t.imin, t.imax)
	b.tw = nil
	return nil
}

func (b *tableCompactionBuilder) cleanup() {
	if b.tw != nil {
		b.tw.drop()
		b.tw = nil
	}
}

func (b *tableCompactionBuilder) run(cnt *compactionTransactCounter) error {
	snapResumed := b.snapIter > 0
	hasLastUkey := b.snapHasLastUkey // The key might has zero length, so this is necessary.
	lastUkey := append([]byte{}, b.snapLastUkey...)
	lastSeq := b.snapLastSeq
	b.kerrCnt = b.snapKerrCnt
	b.dropCnt = b.snapDropCnt
	// Restore compaction state.
	b.c.restore()

	defer b.cleanup()

	b.stat1.startTimer()
	defer b.stat1.stopTimer()

	iter := b.c.newIterator()
	defer iter.Release()
	for i := 0; iter.Next(); i++ {
		// Incr transact counter.
		cnt.incr()

		// Skip until last state.
		if i < b.snapIter {
			continue
		}

		resumed := false
		if snapResumed {
			resumed = true
			snapResumed = false
		}

		ikey := iter.Key()
		ukey, seq, kt, kerr := parseInternalKey(ikey)

		if kerr == nil {
			shouldStop := !resumed && b.c.shouldStopBefore(ikey)

			if !hasLastUkey || b.s.icmp.uCompare(lastUkey, ukey) != 0 {
				// First occurrence of this user key.

				// Only rotate tables if ukey doesn't hop across.
				if b.tw != nil && (shouldStop || b.needFlush()) {
					if err := b.flush(); err != nil {
						return err
					}

					// Creates snapshot of the state.
					b.c.save()
					b.snapHasLastUkey = hasLastUkey
					b.snapLastUkey = append(b.snapLastUkey[:0], lastUkey...)
					b.snapLastSeq = lastSeq
					b.snapIter = i
					b.snapKerrCnt = b.kerrCnt
					b.snapDropCnt = b.dropCnt
				}

				hasLastUkey = true
				lastUkey = append(lastUkey[:0], ukey...)
				lastSeq = keyMaxSeq
			}

			switch {
			case lastSeq <= b.minSeq:
				// Dropped because newer entry for same user key exist
				fallthrough // (A)
			case kt == keyTypeDel && seq <= b.minSeq && b.c.baseLevelForKey(lastUkey):
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger seq numbers
				// (3) data in layers that are being compacted here and have
				//     smaller seq numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				lastSeq = seq
				b.dropCnt++
				continue
			default:
				lastSeq = seq
			}
		} else {
			if b.strict {
				return kerr
			}

			// Don't drop corrupted keys.
			hasLastUkey = false
			lastUkey = lastUkey[:0]
			lastSeq = keyMaxSeq
			b.kerrCnt++
		}

		if err := b.appendKV(ikey, iter.Value()); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Finish last table.
	if b.tw != nil && !b.tw.empty() {
		return b.flush()
	}
	return nil
}

func (b *tableCompactionBuilder) revert() error {
	for _, at := range b.rec.addedTables {
		b.s.logf("table@build revert @%d", at.num)
		if err := b.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: at.num}); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) tableCompaction(c *compaction, noTrivial bool, done func(*compaction)) {
	defer c.release()
	defer func() {
		if done != nil {
			done(c)
		}
	}()

	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax)

	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.sourceLevel+1, t)
		db.compactionCommit("table-move", rec)
		return
	}

	var stats [2]cStatStaging
	for i, tables := range c.levels {
		for _, t := range tables {
			stats[i].read += t.size
			// Insert deleted tables into record
			rec.delTable(c.sourceLevel+i, t.fd.Num)
		}
	}
	sourceSize := int(stats[0].read + stats[1].read)
	minSeq := db.minSeq()
	db.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)

	b := &tableCompactionBuilder{
		db:        db,
		s:         db.s,
		c:         c,
		rec:       rec,
		stat1:     &stats[1],
		minSeq:    minSeq,
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
	}
	db.compactionTransact("table@build", b)

	// Commit.
	stats[1].startTimer()
	db.compactionCommit("table", rec)
	stats[1].stopTimer()

	resultSize := int(stats[1].write)
	db.logf("table@compaction committed F%s S%s Ke·%d D·%d T·%v", sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), b.kerrCnt, b.dropCnt, stats[1].duration)

	// Save compaction stats
	for i := range stats {
		db.compStats.addStat(c.sourceLevel+1, &stats[i])
	}
	switch c.typ {
	case level0Compaction:
		atomic.AddUint32(&db.level0Comp, 1)
	case nonLevel0Compaction:
		atomic.AddUint32(&db.nonLevel0Comp, 1)
	case seekCompaction:
		atomic.AddUint32(&db.seekComp, 1)
	}
}

func (db *DB) tableRangeCompaction(level int, umin, umax []byte) error {
	db.logf("table@compaction range L%d %q:%q", level, umin, umax)
	if level >= 0 {
		if c := db.s.getCompactionRange(level, umin, umax, true); c != nil {
			db.tableCompaction(c, true, nil)
		}
	} else {
		// Retry until nothing to compact.
		for {
			compacted := false

			// Scan for maximum level with overlapped tables.
			v := db.s.version()
			m := 1
			for i := m; i < len(v.levels); i++ {
				tables := v.levels[i]
				if tables.overlaps(db.s.icmp, umin, umax, false) {
					m = i
				}
			}
			v.release()

			for level := 0; level < m; level++ {
				if c := db.s.getCompactionRange(level, umin, umax, false); c != nil {
					db.tableCompaction(c, true, nil)
					compacted = true
				}
			}

			if !compacted {
				break
			}
		}
	}

	return nil
}

// tableNeedCompaction returns the indicator whether system needs compaction.
// If so, then the relevant level or target table(is nil if the normal table
// compaction is required) will be returned.
func (db *DB) tableNeedCompaction(ctx *compactionContext) (needCompact bool, level int, table *tFile) {
	v := db.s.version()
	defer v.release()

	return v.needCompaction(ctx)
}

// resumeWrite returns an indicator whether we should resume write operation if enough level0 files are compacted.
func (db *DB) resumeWrite() bool {
	v := db.s.version()
	defer v.release()
	if v.tLen(0) < db.s.o.GetWriteL0PauseTrigger() {
		return true
	}
	return false
}

func (db *DB) pauseCompaction(ch <-chan struct{}) {
	select {
	case <-ch:
	case <-db.closeC:
		db.compactionExitTransact()
	}
}

type cCmd interface {
	ack(err error)
}

type cAuto struct {
	// Note for table compaction, an non-empty ackC
	// represents it's a compaction waiting command.
	ackC chan<- error

	// Flag whether the ack should only be sent when
	// all compactions finished. Used for testing only.
	full bool
}

func (r cAuto) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

type cRange struct {
	level    int
	min, max []byte
	ackC     chan<- error
}

func (r cRange) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

// This will trigger auto compaction but will not wait for it.
func (db *DB) compTrigger(compC chan<- cCmd) {
	select {
	case compC <- cAuto{}:
	default:
	}
}

// This will trigger auto compaction and wait for all compaction to be done.
// Note it's only used in testing.
func (db *DB) waitAllTableComp() (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case db.tcompCmdC <- cAuto{ackC: ch, full: true}:
	case err = <-db.compErrC:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

// This will trigger auto compaction and/or wait for all compaction to be done.
func (db *DB) compTriggerWait(compC chan<- cCmd) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cAuto{ackC: ch}:
	case err = <-db.compErrC:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

// Send range compaction request.
func (db *DB) compTriggerRange(compC chan<- cCmd, level int, min, max []byte) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cRange{level, min, max, ch}:
	case err := <-db.compErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

func (db *DB) mCompaction() {
	var x cCmd

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		select {
		case x = <-db.mcompCmdC:
			switch x.(type) {
			case cAuto:
				db.memCompaction()
				x.ack(nil)
				x = nil
			default:
				panic("leveldb: unknown command")
			}
		case <-db.closeC:
			return
		}
	}
}

type compactions []*compaction

// Returns true if i smallest key is less than j.
// This used for sort by key in ascending order.
func (cs compactions) lessByKey(icmp *iComparer, i, j int) bool {
	a, b := cs[i], cs[j]
	return icmp.Compare(a.imin, b.imin) < 0
}
func (cs compactions) Len() int      { return len(cs) }
func (cs compactions) Swap(i, j int) { cs[i], cs[j] = cs[j], cs[i] }

// Helper type for sortByKey.
type compactionsSortByKey struct {
	compactions
	icmp *iComparer
}

func (x *compactionsSortByKey) Less(i, j int) bool {
	return x.lessByKey(x.icmp, i, j)
}

type compactionContext struct {
	sorted   map[int][]*compaction
	fifo     map[int][]*compaction
	icmp     *iComparer
	noseek   bool
	denylist map[int]struct{}
}

func (ctx *compactionContext) add(c *compaction) {
	ctx.sorted[c.sourceLevel] = append(ctx.sorted[c.sourceLevel], c)
	sort.Sort(&compactionsSortByKey{
		compactions: ctx.sorted[c.sourceLevel],
		icmp:        ctx.icmp,
	})
	ctx.fifo[c.sourceLevel] = append(ctx.fifo[c.sourceLevel], c)
}

func (ctx *compactionContext) delete(c *compaction) {
	for index, comp := range ctx.sorted[c.sourceLevel] {
		if comp == c {
			ctx.sorted[c.sourceLevel] = append(ctx.sorted[c.sourceLevel][:index], ctx.sorted[c.sourceLevel][index+1:]...)
			break
		}
	}
	for index, comp := range ctx.fifo[c.sourceLevel] {
		if comp == c {
			ctx.fifo[c.sourceLevel] = append(ctx.fifo[c.sourceLevel][:index], ctx.fifo[c.sourceLevel][index+1:]...)
			break
		}
	}
	ctx.reset(c.sourceLevel)
	return
}

// reset resets the denylist and seek flag. If one level-n compaction finishes,
// then it will re-activate the adjacent levels if they are marked unavailable
// before. Besides we always re-activate seek compaction.
func (ctx *compactionContext) reset(level int) {
	if _, exist := ctx.denylist[level]; exist {
		delete(ctx.denylist, level)
	}
	if _, exist := ctx.denylist[level+1]; exist {
		delete(ctx.denylist, level+1)
	}
	if level > 0 {
		if _, exist := ctx.denylist[level-1]; exist {
			delete(ctx.denylist, level-1)
		}
	}
	ctx.noseek = false
}

func (ctx *compactionContext) count() int {
	var total int
	for _, comps := range ctx.sorted {
		total += len(comps)
	}
	return total
}

func (ctx *compactionContext) get(level int) []*compaction {
	return ctx.fifo[level]
}

func (ctx *compactionContext) getSorted(level int) []*compaction {
	return ctx.sorted[level]
}

// removing returns the tables which are acting as the source level input
// in the ongoing compactions. All returned tables are sorted by keys.
func (ctx *compactionContext) removing(level int) tFiles {
	comps := ctx.getSorted(level)
	var v0 tFiles
	for _, comp := range comps {
		v0 = append(v0, comp.levels[0]...)
	}
	return v0
}

// removing returns the tables which are acting as the dest level input
// in the ongoing compactions. All returned tables are sorted by keys.
func (ctx *compactionContext) recreating(level int) tFiles {
	if level == 0 {
		return nil
	}
	comps := ctx.getSorted(level - 1)
	var v1 tFiles
	for _, comp := range comps {
		v1 = append(v1, comp.levels[1]...)
	}
	return v1
}

// tCompaction is the scheduler of table compaction. Here concurrent compactions
// are allowed and scheduled for best performance. Compaction performance is the
// bottleneck of the entire system. Slow compaction will eventually lead to write
// suspension. So this loop will try to maximize the compaction performance by
// selecting isolated files to compact concurrently.
//
// For level0 compaction, concurrency is not allowed. Since we can't guarantee two
// level0 compactions are non-overlapped. But we do see that level0 compaction in
// some sense become the bottleneck, it can slow down/suspend write operations if
// it's not fast enough. Also if level0 compaction can't generate tables fast
// enough, the non-level0 compactors may become idle.
//
// For non-level0 compaction, concurrency is allowed if two compactions are totally
// isolated.
//
// For compaction level selection, it's a little different with single-thread version.
// Now two factors will be considered to select source level: current version and ongoing
// compactions. For level0 if there is already one compaction running, then no more level0
// compaction should be picked. For non-level0, if the total size except the "removing"
// tables and "recreating" tables still exceeds the threshold, it may be picked.
// The "removing" tables refers to the tables which are picked as the source level input
// of ongoing compactions. "recreating" tables refers to the tables which are picked
// as the dest level input of ongoing compactions.
//
// For compaction file selection, it's the core of the entire mechanism. For source level
// we only pick the file which is idle. Idle means it's not the input of other compactions
// (either level n compactions or level n-1 compactions). It's same when picking files in
// parent level. In this way we can ensure all compacting files are isolated with each other.
//
// But in the parent level, there is one difference. Actually for the file in parent level
// which is removing(the input of child level compactions), we can just kick them out and mark
// these compactions as the dependencies. But it will make the code much more complicated.
// Also consider the concurrency is limited, so we just don't accept this kind of compaction.
func (db *DB) tCompaction() {
	var (
		// The maximum number of compactions are allowed to run concurrently.
		// The default value is the CPU core number.
		compLimit = db.s.o.GetCompactionConcurrency()

		// Compaction context includes all ongoing compactions.
		ctx = &compactionContext{
			sorted:   make(map[int][]*compaction),
			fifo:     make(map[int][]*compaction),
			icmp:     db.s.icmp,
			denylist: make(map[int]struct{}),
		}
		done  = make(chan *compaction)
		subWg sync.WaitGroup

		// Various waiting list
		x        cCmd
		waitQ    []cCmd // Waiting list will be activated if the level0 tables less then threshold
		waitAll  []cCmd // Waiting list will be activated iff all compactions have finished.
		rangeCmd cCmd   // Single range compaction waiting channel
	)
	defer func() {
		// Panic catcher for potential range compaction.
		// For all other compactions the panic will be
		// caught in their own routine.
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		subWg.Wait()
		for i := range waitQ {
			waitQ[i].ack(ErrClosed)
			waitQ[i] = nil
		}
		for i := range waitAll {
			waitAll[i].ack(ErrClosed)
			waitAll[i] = nil
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		if rangeCmd != nil {
			rangeCmd.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		// Send ack signal to all waiting channels for resuming
		// db operation(e.g. writes).
		if len(waitQ) > 0 && db.resumeWrite() {
			for i := range waitQ {
				waitQ[i].ack(nil)
				waitQ[i] = nil
			}
			waitQ = waitQ[:0]
		}
		var (
			needCompact bool
			level       int
			table       *tFile
		)
		if ctx.count() < compLimit && rangeCmd == nil {
			needCompact, level, table = db.tableNeedCompaction(ctx)
		}
		if needCompact {
			select {
			case <-db.closeC:
				return
			case x = <-db.tcompCmdC:
			//case ch := <-db.tcompPauseC:
			//	db.pauseCompaction(ch)
			//	continue
			case c := <-done:
				ctx.delete(c)
				continue
			default:
			}
		} else {
			// If there is a pending range compaction, do it right now
			if rangeCmd != nil && ctx.count() == 0 {
				cmd := rangeCmd.(cRange)
				cmd.ack(db.tableRangeCompaction(cmd.level, cmd.min, cmd.max))
				rangeCmd = nil
				continue // Re-loop is necessary, try to spin up more compactions
			}
			// If the waitAll list is not empty, send the ack if all compactions have finished.
			if len(waitAll) != 0 && ctx.count() == 0 {
				for _, wait := range waitAll {
					wait.ack(nil)
					wait = nil
				}
				waitAll = waitAll[:0]
			}
			select {
			case <-db.closeC:
				return
			case x = <-db.tcompCmdC:
			//case ch := <-db.tcompPauseC:
			//	db.pauseCompaction(ch)
			//	continue
			case c := <-done:
				ctx.delete(c)
				continue
			}
		}
		if x != nil {
			switch cmd := x.(type) {
			case cAuto:
				if cmd.full {
					waitAll = append(waitAll, cmd)
				} else if cmd.ackC != nil {
					waitQ = append(waitQ, x)
				}
			case cRange:
				if ctx.count() > 0 {
					rangeCmd = x
				} else {
					x.ack(db.tableRangeCompaction(cmd.level, cmd.min, cmd.max))
				}
			default:
				panic("leveldb: unknown command")
			}
			x = nil
			continue
		}
		db.runCompaction(ctx, level, table, &subWg, done)
	}
}

func (db *DB) runCompaction(ctx *compactionContext, level int, table *tFile, wg *sync.WaitGroup, done chan *compaction) {
	var c *compaction
	if table == nil {
		c = db.s.pickCompactionByLevel(level, ctx)
		if c == nil {
			// We can't pick one more isolated compaction in level n.
			// Mark the entire level as unavailable. In theory it shouldn't
			// happen a lot.
			ctx.denylist[level] = struct{}{}
			return
		}
	} else {
		c = db.s.pickCompactionByTable(level, table, ctx)
		if c == nil {
			// The involved tables are not available now. Mark the seek
			// compaction as unavailable.
			ctx.noseek = true
			return
		}
	}
	ctx.add(c)
	wg.Add(1)

	go func() {
		// Catch the panic in its own goroutine.
		defer func() {
			if x := recover(); x != nil {
				if x != errCompactionTransactExiting {
					panic(x)
				}
			}
		}()
		defer wg.Done()

		db.tableCompaction(c, false, func(c *compaction) {
			select {
			case done <- c:
			case <-db.closeC:
			}
		})
	}()
}
