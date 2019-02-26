package leveldb

import (
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"

	"github.com/onsi/gomega"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/testutil"
)

type testFileRec struct {
	level int
	num   int64
}

func TestVersionStaging(t *testing.T) {
	gomega.RegisterTestingT(t)
	stor := testutil.NewStorage()
	defer stor.Close()
	s, err := newSession(stor, nil)
	if err != nil {
		t.Fatal(err)
	}

	v := newVersion(s)
	v.newStaging()

	tmp := make([]byte, 4)
	mik := func(i uint64) []byte {
		binary.BigEndian.PutUint32(tmp, uint32(i))
		return []byte(makeInternalKey(nil, tmp, 0, keyTypeVal))
	}

	for i, x := range []struct {
		add, del []testFileRec
		trivial  bool
		levels   [][]int64
	}{
		{
			add: []testFileRec{
				{1, 1},
			},
			levels: [][]int64{
				{},
				{1},
			},
		},
		{
			add: []testFileRec{
				{1, 1},
			},
			levels: [][]int64{
				{},
				{1},
			},
		},
		{
			del: []testFileRec{
				{1, 1},
			},
			levels: [][]int64{},
		},
		{
			add: []testFileRec{
				{0, 1},
				{0, 3},
				{0, 2},
				{2, 5},
				{1, 4},
			},
			levels: [][]int64{
				{3, 2, 1},
				{4},
				{5},
			},
		},
		{
			add: []testFileRec{
				{1, 6},
				{2, 5},
			},
			del: []testFileRec{
				{0, 1},
				{0, 4},
			},
			levels: [][]int64{
				{3, 2},
				{4, 6},
				{5},
			},
		},
		{
			del: []testFileRec{
				{0, 3},
				{0, 2},
				{1, 4},
				{1, 6},
				{2, 5},
			},
			levels: [][]int64{},
		},
		{
			add: []testFileRec{
				{0, 1},
			},
			levels: [][]int64{
				{1},
			},
		},
		{
			add: []testFileRec{
				{1, 2},
			},
			levels: [][]int64{
				{1},
				{2},
			},
		},
		{
			add: []testFileRec{
				{0, 3},
			},
			levels: [][]int64{
				{3, 1},
				{2},
			},
		},
		{
			add: []testFileRec{
				{6, 9},
			},
			levels: [][]int64{
				{3, 1},
				{2},
				{},
				{},
				{},
				{},
				{9},
			},
		},
		{
			del: []testFileRec{
				{6, 9},
			},
			levels: [][]int64{
				{3, 1},
				{2},
			},
		},
		// memory compaction
		{
			add: []testFileRec{
				{0, 5},
			},
			trivial: true,
			levels: [][]int64{
				{5, 3, 1},
				{2},
			},
		},
		// memory compaction
		{
			add: []testFileRec{
				{0, 4},
			},
			trivial: true,
			levels: [][]int64{
				{5, 4, 3, 1},
				{2},
			},
		},
		// table compaction
		{
			add: []testFileRec{
				{1, 6},
				{1, 7},
				{1, 8},
			},
			del: []testFileRec{
				{0, 3},
				{0, 4},
				{0, 5},
			},
			trivial: true,
			levels: [][]int64{
				{1},
				{2, 6, 7, 8},
			},
		},
	} {
		rec := &sessionRecord{}
		for _, f := range x.add {
			ik := mik(uint64(f.num))
			rec.addTable(f.level, f.num, 1, ik, ik)
		}
		for _, f := range x.del {
			rec.delTable(f.level, f.num)
		}
		vs := v.newStaging()
		vs.commit(rec)
		v = vs.finish(x.trivial)
		if len(v.levels) != len(x.levels) {
			t.Fatalf("#%d: invalid level count: want=%d got=%d", i, len(x.levels), len(v.levels))
		}
		for j, want := range x.levels {
			tables := v.levels[j]
			if len(want) != len(tables) {
				t.Fatalf("#%d.%d: invalid tables count: want=%d got=%d", i, j, len(want), len(tables))
			}
			got := make([]int64, len(tables))
			for k, t := range tables {
				got[k] = t.fd.Num
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("#%d.%d: invalid tables: want=%v got=%v", i, j, want, got)
			}
		}
	}
}

func BenchmarkVersionStagingNonTrivial(b *testing.B) {
	benchmarkVersionStaging(b, false, 100000)
}

func BenchmarkVersionStagingTrivial(b *testing.B) {
	benchmarkVersionStaging(b, true, 100000)
}

func benchmarkVersionStaging(b *testing.B, trivial bool, size int) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	s, err := newSession(stor, nil)
	if err != nil {
		b.Fatal(err)
	}

	v := newVersion(s)
	v.newStaging()

	tmp := make([]byte, 4)
	mik := func(i uint64) []byte {
		binary.BigEndian.PutUint32(tmp, uint32(i))
		return []byte(makeInternalKey(nil, tmp, 0, keyTypeVal))
	}

	rec := &sessionRecord{}
	for i := 0; i < size; i++ {
		ik := mik(uint64(i))
		rec.addTable(1, int64(i), 1, ik, ik)
	}
	vs := v.newStaging()
	vs.commit(rec)
	v = vs.finish(false)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rec := &sessionRecord{}
		index := rand.Intn(size)
		ik := mik(uint64(index))

		cnt := 0
		for j := index; j < size && cnt <= 3; j++ {
			rec.addTable(1, int64(i), 1, ik, ik)
			cnt += 1
		}
		vs := v.newStaging()
		vs.commit(rec)
		vs.finish(trivial)
	}
}
