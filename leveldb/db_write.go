// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (db *DB) writeJournal(batches []*Batch, seq uint64, sync bool) error {
	wr, err := db.journal.Next()
	if err != nil {
		return err
	}
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil {
		return err
	}
	if err := db.journal.Flush(); err != nil {
		return err
	}
	if sync {
		return db.journalWriter.Sync()
	}
	return nil
}

func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	err = db.compTriggerWait(db.mcompCmdC) // 这一步成功之后，就不该还有 frozen memDB
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.
	mem, err = db.newMem(n)
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				// 既然之前已经 wait 过 compaction了，那么这时就不该还有 frozen memDB
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	if wait {
		err = db.compTriggerWait(db.mcompCmdC)
	} else {
		db.compTrigger(db.mcompCmdC)
	}
	return
}

// 如果 db 写入数据量过快，则会触发 throttle
func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {
	delayed := false
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger()
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger()
	flush := func() (retry bool) {
		mdb = db.getEffectiveMem()
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			if retry {
				mdb.decref()
				mdb = nil
			}
		}()
		tLen := db.s.tLen(0) // 当前版本 Level-0 的 SST 个数s
		mdbFree = mdb.Free() // cap - size，即 mdb 还有多少剩下的空间s
		switch {
		case tLen >= slowdownTrigger && !delayed:
			// level-0 的文件个数超出了阈值，默认为 slowdownTrigger=8，会减缓写入速度一次
			delayed = true
			time.Sleep(time.Millisecond)
		case mdbFree >= n:
			// mdb 的剩余空间足够容纳这么多数据量
			return false
		case tLen >= pauseTrigger:
			// 注意：此时 mdbFree < n，也就是说 mdb 目前不足以容纳 n 规模的数据
			delayed = true
			// Set the write paused flag explicitly.
			atomic.StoreInt32(&db.inWritePaused, 1)
			err = db.compTriggerWait(db.tcompCmdC)
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}

			// 注意：当 err==nil 时，会 return true，也就是继续重试，如果还是 tLen >= pausedTrigger，则继续触发 L0 compaction
			// 直到 Level-0 的 SST 数目不 block write
		default:
			// 此时，mdb 的空间不足以容纳 n 规模的数据，且 write 没有被 Level-0 的 compaction block
			// Allow memdb to grow if it has no entry.
			if mdb.Len() == 0 {
				mdbFree = n
			} else {
				mdb.decref()
				mdb, err = db.rotateMem(n, false) // 不会等待 memDB compaction 完成
				if err == nil {
					mdbFree = mdb.Free()
				} else {
					mdbFree = 0
				}
			}
			return false
		}
		return true
	}
	start := time.Now()
	for flush() {
	}
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

type writeMerge struct {
	sync       bool
	batch      *Batch
	keyType    keyType
	key, value []byte
}

func (db *DB) unlockWrite(overflow bool, merged int, err error) {
	for i := 0; i < merged; i++ {
		db.writeAckC <- err
	}
	if overflow {
		// Pass lock to the next write (that failed to merge).
		db.writeMergedC <- false
	} else {
		// Release lock.
		<-db.writeLockC
	}
}

// ourBatch is batch that we can modify.
func (db *DB) writeLocked(batch, ourBatch *Batch, merge, sync bool) error {
	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	mdb, mdbFree, err := db.flush(batch.internalLen)
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref()

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch}
	)

	if merge {
		// Merge limit.
		var mergeLimit int
		if batch.internalLen > 128<<10 {
			mergeLimit = (1 << 20) - batch.internalLen
		} else {
			mergeLimit = 128 << 10
		}
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap {
			mergeLimit = mergeCap
		}

	merge:
		// 只要容量还有剩余，就 merge 下一个 Write，直到超出容量上限
		// 对于 merge 进来的 Write，通知负责 Write 的一端这个 Write 已经被 merge，于是负责的那一方可以不用管了，我们这里会负责到底，直到 Write 成功或失败
		// 对于因为超出了容量，merge 不进来的那个 Write，我们通知它的负责人“这个 Write 得由你自己负责了，没赶上上一波发车”，
		// Write Lock 交接给那个负责人，它可以继续执行从它开始的 merge write
		for mergeLimit > 0 {
			select {
			case incoming := <-db.writeMergeC: // 不断合并可得的写请求，可能是要合并一个 batch write，或者合并一个 put
				if incoming.batch != nil {
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit {
						overflow = true
						break merge
					}
					batches = append(batches, incoming.batch) // 注意：这里是整个 batch 合并，保证 batch 执行的原子性，不会存在 batch 一部分成功了，另一部分失败了的情况
					mergeLimit -= incoming.batch.internalLen
				} else {
					// Merge put.
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						overflow = true
						break merge
					}
					if ourBatch == nil {
						ourBatch = db.batchPool.Get().(*Batch)
						ourBatch.Reset()
						batches = append(batches, ourBatch)
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync
				merged++
				db.writeMergedC <- true // 通知各个 Write，这次操作已经被 merge 了

			default:
				break merge
			}
		}
	}

	// Release ourBatch if any.
	if ourBatch != nil {
		defer db.batchPool.Put(ourBatch)
	}

	// Seq number.
	seq := db.seq + 1

	// Write journal.
	// 先写 journal，journal 写成功了就是 Write 成功了
	// journal 的 seq 写入的时候用的是相同的 seq
	// journal 包含了所有的 batches，同时成功或失败
	if err := db.writeJournal(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}

	// Put batches.
	for _, batch := range batches {
		// mdb 写入的时候不应该出现问题
		if err := batch.putMem(seq, mdb.DB); err != nil {
			panic(err)
		}
		seq += uint64(batch.Len())
	}

	// Incr seq number.
	db.addSeq(uint64(batchesLen(batches)))

	// Rotate memdb if it's reach the threshold.
	if batch.internalLen >= mdbFree {
		if _, err := db.rotateMem(0, false); err != nil {
			db.unlockWrite(overflow, merged, err)
			return err
		}
	}

	db.unlockWrite(overflow, merged, nil)
	return nil
}

// Write apply the given batch to the DB. The batch records will be applied
// sequentially. Write might be used concurrently, when used concurrently and
// batch is small enough, write will try to merge the batches. Set NoWriteMerge
// option to true to disable write merge.
//
// It is safe to modify the contents of the arguments after Write returns but
// not before. Write will not modify content of the batch.
func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}

	// If the batch size is larger than write buffer, it may justified to write
	// using transaction instead. Using transaction the batch will be written
	// into tables directly, skipping the journaling.
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		return tr.Commit()
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked(batch, nil, merge, sync)
}

func (db *DB) putRec(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil {
		return err
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	batch := db.batchPool.Get().(*Batch)
	batch.Reset()
	batch.appendRec(kt, key, value)

	// 这里传参 batch 和 ourBatch 都是同一个 `batch`，所以函数内 ourBatch 在合并 put 的时候，就相当于那个共同的 `batch` 把 put 合并掉了
	return db.writeLocked(batch, batch, merge, sync)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map. Write merge also applies for Put, see
// Write.
//
// It is safe to modify the contents of the arguments after Put returns but not
// before.
func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeVal, key, value, wo)
}

// Delete deletes the value for the given key. Delete will not returns error if
// key doesn't exist. Write merge also applies for Delete, see Write.
//
// It is safe to modify the contents of the arguments after Delete returns but
// not before.
func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeDel, key, nil, wo)
}

// 注意：这里的 min, max 都是 user key，是使用 user key 来判定是否重叠
func isMemOverlaps(icmp *iComparer, mem *memdb.DB, min, max []byte) bool {
	iter := mem.NewIterator(nil)
	defer iter.Release()
	return (max == nil || (iter.First() && icmp.uCompare(max, internalKey(iter.Key()).ukey()) >= 0)) &&
		(min == nil || (iter.Last() && icmp.uCompare(min, internalKey(iter.Key()).ukey()) <= 0))
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
func (db *DB) CompactRange(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		// 获得了 Write lock，此时 DB 的 Write 操作会被 block
		// 获得这个锁是为了完成 mem compaction
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdC); err != nil {
			// 等待 mem compaction 完毕，也就是刚才在 rotateMem 时被转化为 frozen memtable 的（也就是执行 CompactRange 时的当前 memtable）完成 mem compaction
			// 此时就完成了 memtable 的 compaction，所以 Write lock 就被释放了
			return err
		}
	} else {
		// memtable 与 range 不重叠，不需要进行 mem compaction，所以直接释放锁
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdC, -1, r.Start, r.Limit)
}

// SetReadOnly makes DB read-only. It will stay read-only until reopened.
func (db *DB) SetReadOnly() error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		db.compWriteLocking = true
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Set compaction read-only.
	select {
	case db.compErrSetC <- ErrReadOnly:
	case perr := <-db.compPerErrC:
		return perr
	case <-db.closeC:
		return ErrClosed
	}

	return nil
}
