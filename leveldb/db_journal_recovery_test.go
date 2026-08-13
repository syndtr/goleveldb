// Copyright (c) 2026, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Regression tests for journal recovery dropping records that the reference
// C++ implementation replays, and for the read-write open then deleting the
// journal so the loss becomes permanent.
//
// The trigger is a disagreement about what the manifest's last-sequence field
// means relative to the journal the manifest names.
//
//	                     journal 000003 opened here
//	                     |
//	seq  ... 9  10 | 11  12  13  14  15  16 ... 20
//	               |         batches in journal 000003
//	               ^                   ^
//	               frozenSeq           liveSeq
//	               what goleveldb      what the reference
//	               records             records
//
// The reference implementation reaches the second state routinely. When a
// memtable fills it opens a new journal and keeps accepting writes into it;
// the flush of the old memtable runs in the background with the lock released.
// When that flush finishes, CompactMemTable records log_number = the journal
// currently being written (db_impl.cc), and LogAndApply records last_sequence =
// last_sequence_, the live counter, which the concurrent writes have already
// advanced (version_set.cc). So the manifest names a journal whose first record
// is older than the sequence the manifest carries.
//
// The reference does not mind: RecoverLogFile replays every batch in the
// journal and only ever raises max_sequence (db_impl.cc).
//
// goleveldb did mind. It seeds db.seq from the manifest and decodeBatchToMem
// rejected any batch below it as corrupt:
//
//	if seq < expectSeq {
//	        return 0, 0, newErrBatchCorrupted("invalid sequence number")
//	}
//
// and recoverJournal skips corrupt batches without failing the open, because
// opt.StrictJournal is set neither by DefaultStrict nor by NoStrict -- so the
// records went with no error and no trace an application could observe.
// goleveldb's own writers never hit this, because a goleveldb memtable flush
// records frozenSeq, the sequence at rotation, rather than the live counter.
// Only foreign-written directories were affected.

const (
	// The journal holds one batch per sequence in [firstJournalSeq,
	// lastJournalSeq]; each batch writes the single key named for its sequence.
	firstJournalSeq = 11
	lastJournalSeq  = 20

	// frozenSeqFixture is the sequence the rotated memtable ended at -- what a
	// goleveldb writer puts in the manifest.
	frozenSeqFixture = 10

	// liveSeqFixture is where concurrent writes had taken the live counter by
	// the time the background flush wrote the manifest -- what the reference
	// puts there.
	liveSeqFixture = 15

	fixtureManifestNum = 2
	fixtureJournalNum  = 3
	fixtureNextFileNum = 4
)

// Tags in a VersionEdit record, from version_edit.cc. session_record.go decodes
// the same numbers; they are spelled out here so the fixture describes the
// on-disk format rather than the code under test.
const (
	fixtureTagComparator     = 1
	fixtureTagLogNumber      = 2
	fixtureTagNextFileNumber = 3
	fixtureTagLastSequence   = 4
)

// Record types inside a write batch, from dbformat.h. Same reasoning as above:
// these are keyTypeDel and keyTypeVal, written out independently.
const (
	fixtureTypeDeletion = 0
	fixtureTypeValue    = 1
)

// fixtureComparator is the only comparator name goleveldb will open a directory
// under; a mismatch is rejected outright in session.recover.
const fixtureComparator = "leveldb.BytewiseComparator"

// fixtureEntry is one key/value record inside a fixtureBatch.
type fixtureEntry struct {
	key    string
	value  string
	delete bool // encode as a deletion; value is ignored
}

// fixtureBatch is one write batch as it appears in a journal file. seq is the
// sequence number assigned to its first entry.
type fixtureBatch struct {
	seq     uint64
	entries []fixtureEntry
}

// recoveryFixture describes a LevelDB directory written from scratch: one
// manifest naming one journal, and the batches that journal holds. The
// interesting states are the ones where lastSequence does not sit where a
// goleveldb writer would have put it.
//
// The three file formats are small enough to write directly:
//
//   - CURRENT holds the current manifest's file name and a newline.
//   - MANIFEST-N is a journal-framed file holding VersionEdit records, each a
//     sequence of varint-tagged fields (version_edit.cc).
//   - N.log is a journal-framed file holding write batches: an 8-byte sequence
//     number, a 4-byte record count, then that many key/value records
//     (write_batch.cc).
//
// Journal framing itself is not what is under test, so journal.Writer writes it.
type recoveryFixture struct {
	manifestNum  uint64
	journalNum   uint64
	nextFileNum  uint64
	lastSequence uint64
	batches      []fixtureBatch
}

func (f recoveryFixture) journalName() string {
	return fmt.Sprintf("%06d.log", f.journalNum)
}

func (f recoveryFixture) manifestName() string {
	return fmt.Sprintf("MANIFEST-%06d", f.manifestNum)
}

// write creates dir and writes the fixture's CURRENT, manifest and journal.
func (f recoveryFixture) write(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create fixture dir: %v", err)
	}
	if err := f.writeManifest(dir); err != nil {
		return err
	}
	if err := f.writeJournal(dir); err != nil {
		return err
	}
	current := []byte(f.manifestName() + "\n")
	if err := os.WriteFile(filepath.Join(dir, "CURRENT"), current, 0o644); err != nil {
		return fmt.Errorf("write CURRENT: %v", err)
	}
	return nil
}

// writeManifest writes a single VersionEdit carrying the four fields goleveldb
// requires to be present: comparator, log number, next file number and last
// sequence. A manifest omitting any of them is rejected as corrupt rather than
// recovered.
func (f recoveryFixture) writeManifest(dir string) error {
	var rec bytes.Buffer
	fixturePutUvarint(&rec, fixtureTagComparator)
	fixturePutLenPrefixed(&rec, fixtureComparator)
	fixturePutUvarint(&rec, fixtureTagLogNumber)
	fixturePutUvarint(&rec, f.journalNum)
	fixturePutUvarint(&rec, fixtureTagNextFileNumber)
	fixturePutUvarint(&rec, f.nextFileNum)
	fixturePutUvarint(&rec, fixtureTagLastSequence)
	fixturePutUvarint(&rec, f.lastSequence)

	return fixtureWriteJournalFile(filepath.Join(dir, f.manifestName()), [][]byte{rec.Bytes()})
}

// writeJournal writes the fixture's batches into the journal file, one journal
// record per batch, which is how a write per transaction lands on disk.
func (f recoveryFixture) writeJournal(dir string) error {
	records := make([][]byte, 0, len(f.batches))
	for i, b := range f.batches {
		rec, err := fixtureEncodeBatch(b)
		if err != nil {
			return fmt.Errorf("encode batch %d: %v", i, err)
		}
		records = append(records, rec)
	}
	return fixtureWriteJournalFile(filepath.Join(dir, f.journalName()), records)
}

// fixtureEncodeBatch lays out one write batch: sequence number, record count,
// then the records themselves. The count field is 32 bits wide on disk.
func fixtureEncodeBatch(b fixtureBatch) ([]byte, error) {
	if uint64(len(b.entries)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%d entries exceeds the batch count field", len(b.entries))
	}

	var buf bytes.Buffer
	var header [batchHeaderLen]byte
	binary.LittleEndian.PutUint64(header[:8], b.seq)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(b.entries)))
	buf.Write(header[:])

	for _, e := range b.entries {
		if e.delete {
			buf.WriteByte(fixtureTypeDeletion)
			fixturePutLenPrefixed(&buf, e.key)
			continue
		}
		buf.WriteByte(fixtureTypeValue)
		fixturePutLenPrefixed(&buf, e.key)
		fixturePutLenPrefixed(&buf, e.value)
	}
	return buf.Bytes(), nil
}

// fixtureWriteJournalFile writes records into path using LevelDB's journal
// framing, which both the manifest and the write-ahead log use.
func fixtureWriteJournalFile(path string, records [][]byte) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %v", filepath.Base(path), err)
	}

	w := journal.NewWriter(file)
	for i, rec := range records {
		dst, err := w.Next()
		if err != nil {
			file.Close()
			return fmt.Errorf("start record %d in %s: %v", i, filepath.Base(path), err)
		}
		if _, err := dst.Write(rec); err != nil {
			file.Close()
			return fmt.Errorf("write record %d in %s: %v", i, filepath.Base(path), err)
		}
	}
	if err := w.Close(); err != nil {
		file.Close()
		return fmt.Errorf("flush %s: %v", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %v", filepath.Base(path), err)
	}
	return nil
}

func fixturePutUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func fixturePutLenPrefixed(buf *bytes.Buffer, s string) {
	fixturePutUvarint(buf, uint64(len(s)))
	buf.WriteString(s)
}

func fixtureSeqKey(seq uint64) string { return fmt.Sprintf("key-%02d", seq) }

// newRecoveryFixture builds the directory described at the top of this file,
// with the manifest's last-sequence field as the single variable under test.
func newRecoveryFixture(lastSequence uint64) recoveryFixture {
	f := recoveryFixture{
		manifestNum:  fixtureManifestNum,
		journalNum:   fixtureJournalNum,
		nextFileNum:  fixtureNextFileNum,
		lastSequence: lastSequence,
	}
	for seq := uint64(firstJournalSeq); seq <= lastJournalSeq; seq++ {
		f.batches = append(f.batches, fixtureBatch{
			seq:     seq,
			entries: []fixtureEntry{{key: fixtureSeqKey(seq), value: "v"}},
		})
	}
	return f
}

// writeRecoveryFixture writes f into a fresh temporary directory.
func writeRecoveryFixture(t *testing.T, f recoveryFixture) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	if err := f.write(dir); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return dir
}

// recoveredFixtureKeys opens dir and returns every key the database reports.
func recoveredFixtureKeys(t *testing.T, dir string, o *opt.Options) []string {
	t.Helper()
	db, err := OpenFile(dir, o)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close %s: %v", dir, err)
		}
	}()

	var keys []string
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		t.Fatalf("iterate %s: %v", dir, err)
	}
	return keys
}

// wantFixtureKeys lists the keys for sequences in [from, lastJournalSeq].
func wantFixtureKeys(from uint64) []string {
	var keys []string
	for seq := from; seq <= lastJournalSeq; seq++ {
		keys = append(keys, fixtureSeqKey(seq))
	}
	return keys
}

func assertFixtureKeys(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("recovered %d keys, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("recovered key %d = %q, want %q\n got: %v\nwant: %v", i, got[i], want[i], got, want)
			return
		}
	}
}

// TestJournalRecoveryBelowManifestSequence is the reproducer. Before the fix,
// the reference-shaped fixture came back missing every batch below the
// manifest's last-sequence field, with no error; both shapes must return all
// ten keys.
func TestJournalRecoveryBelowManifestSequence(t *testing.T) {
	writers := []struct {
		name         string
		lastSequence uint64
	}{
		{"goleveldb_writer_records_the_frozen_sequence", frozenSeqFixture},
		{"reference_writer_records_the_live_sequence", liveSeqFixture},
	}
	// Relaxing strictness makes no difference, and cannot: opt.StrictJournal is
	// absent from DefaultStrict and cleared by NoStrict alike, so both take the
	// silent-skip branch.
	modes := []struct {
		name    string
		options *opt.Options
	}{
		{"default_options", &opt.Options{ReadOnly: true}},
		{"NoStrict", &opt.Options{ReadOnly: true, Strict: opt.NoStrict}},
	}

	for _, w := range writers {
		for _, m := range modes {
			t.Run(w.name+"/"+m.name, func(t *testing.T) {
				dir := writeRecoveryFixture(t, newRecoveryFixture(w.lastSequence))
				got := recoveredFixtureKeys(t, dir, m.options)
				// Every batch in the journal must come back, whatever the
				// manifest's last-sequence field says. The reference returns
				// all ten in both shapes.
				assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq))
			})
		}
	}
}

// TestJournalRecoveryStrictAcceptsBelowManifestSequence guards against the fix
// being mistaken for leniency. Before it, opening this fixture under
// opt.StrictJournal failed with "invalid sequence number" -- the same rejection
// DefaultStrict turned into a silent skip. The batches are not corrupt, so the
// strictest setting must accept them and return everything.
func TestJournalRecoveryStrictAcceptsBelowManifestSequence(t *testing.T) {
	dir := writeRecoveryFixture(t, newRecoveryFixture(liveSeqFixture))
	got := recoveredFixtureKeys(t, dir, &opt.Options{ReadOnly: true, Strict: opt.StrictJournal})
	assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq))
}

// TestJournalRecoveryReadWriteOpenPreservesRecords covers what made the bug
// destructive rather than merely wrong: the skipped records were not just
// invisible, they were destroyed. recoverJournal ends by committing a manifest
// that names a fresh journal and removing the old one, so anything it failed to
// read became unreachable for every later reader, the reference implementation
// included.
//
// The journal is still deleted -- that is ordinary LevelDB behaviour and this
// test asserts it, because the deletion is what makes a recovery mistake
// irreversible. What must hold is that everything in it was recovered first.
func TestJournalRecoveryReadWriteOpenPreservesRecords(t *testing.T) {
	fixture := newRecoveryFixture(liveSeqFixture)
	dir := writeRecoveryFixture(t, fixture)
	journalPath := filepath.Join(dir, fixture.journalName())

	db, err := OpenFile(dir, nil) // read-write
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Errorf("journal %s survived the read-write open (stat error: %v); "+
			"recovery mistakes would be reversible and this test no longer covers the bug",
			fixture.journalName(), err)
	}

	// Nothing names that journal any more, so this is the last word on its
	// contents.
	got := recoveredFixtureKeys(t, dir, &opt.Options{ReadOnly: true})
	assertFixtureKeys(t, got, wantFixtureKeys(firstJournalSeq))
}

// TestJournalRecoveryDiscardsNothing reads the trace goleveldb leaves in the
// database's LOG file. Before the fix it held one "invalid sequence number
// (skipped)" line per lost record, which was the only signal an operator ever
// got. Recovery must discard nothing.
//
// This is what makes the counting tests trustworthy: a count can come out right
// while records are dropped and re-added elsewhere; a clean LOG says the batches
// were never rejected in the first place.
func TestJournalRecoveryDiscardsNothing(t *testing.T) {
	dir := writeRecoveryFixture(t, newRecoveryFixture(liveSeqFixture))

	db, err := OpenFile(dir, nil)
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	logBytes, err := os.ReadFile(filepath.Join(dir, "LOG"))
	if err != nil {
		t.Fatalf("read LOG: %v", err)
	}
	for _, unwanted := range []string{"invalid sequence number", "(skipped)"} {
		if strings.Contains(string(logBytes), unwanted) {
			t.Errorf("LOG reports %q; recovery is still discarding journal records:\n%s", unwanted, logBytes)
		}
	}
}
