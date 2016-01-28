package leveldb

import (
	"encoding/binary"
	"reflect"
	"testing"
)

type testFileRec struct {
	level int
	num   uint64
}

func TestVersionStaging(t *testing.T) {
	stor := newTestStorage(t)
	defer stor.Close()
	s, err := newSession(stor, nil)
	if err != nil {
		t.Fatal(err)
	}

	v := newVersion(s)
	v.newStaging()

	tmp := make([]byte, 4)
	makeIKey := func(i uint64) []byte {
		binary.BigEndian.PutUint32(tmp, uint32(i))
		return []byte(newIkey(tmp, 0, ktVal))
	}

	for i, x := range []struct {
		add, del []testFileRec
		levels   [][]uint64
	}{
		{
			add: []testFileRec{
				{1, 1},
			},
			levels: [][]uint64{
				{},
				{1},
			},
		},
		{
			add: []testFileRec{
				{1, 1},
			},
			levels: [][]uint64{
				{},
				{1},
			},
		},
		{
			del: []testFileRec{
				{1, 1},
			},
			levels: [][]uint64{},
		},
		{
			add: []testFileRec{
				{0, 1},
				{0, 3},
				{0, 2},
				{2, 5},
				{1, 4},
			},
			levels: [][]uint64{
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
			levels: [][]uint64{
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
			levels: [][]uint64{},
		},
		{
			add: []testFileRec{
				{0, 1},
			},
			levels: [][]uint64{
				{1},
			},
		},
		{
			add: []testFileRec{
				{1, 2},
			},
			levels: [][]uint64{
				{1},
				{2},
			},
		},
		{
			add: []testFileRec{
				{0, 3},
			},
			levels: [][]uint64{
				{3, 1},
				{2},
			},
		},
		{
			add: []testFileRec{
				{6, 9},
			},
			levels: [][]uint64{
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
			levels: [][]uint64{
				{3, 1},
				{2},
			},
		},
	} {
		rec := &sessionRecord{}
		for _, f := range x.add {
			ik := makeIKey(f.num)
			rec.addTable(f.level, f.num, 1, ik, ik)
		}
		for _, f := range x.del {
			rec.delTable(f.level, f.num)
		}
		vs := v.newStaging()
		vs.commit(rec)
		v = vs.finish()
		if len(v.levels) != len(x.levels) {
			t.Fatalf("#%d: invalid level count: want=%d got=%d", i, len(x.levels), len(v.levels))
		}
		for j, want := range x.levels {
			tables := v.levels[j]
			if len(want) != len(tables) {
				t.Fatalf("#%d.%d: invalid tables count: want=%d got=%d", i, j, len(want), len(tables))
			}
			got := make([]uint64, len(tables))
			for k, t := range tables {
				got[k] = t.file.Num()
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("#%d.%d: invalid tables: want=%v got=%v", i, j, want, got)
			}
		}
	}
}
