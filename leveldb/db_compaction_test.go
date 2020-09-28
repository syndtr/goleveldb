// Copyright (c) 2020, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"testing"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

func increseKey(key []byte) []byte {
	for i := len(key) - 1; i >= 0; i-- {
		key[i]++
		if key[i] != 0x0 {
			break
		}
	}
	return key
}

func copyBytes(b []byte) (copiedBytes []byte) {
	if b == nil {
		return nil
	}
	copiedBytes = make([]byte, len(b))
	copy(copiedBytes, b)
	return
}

func testCompaction(level int, sn int, start internalKey) *compaction {
	var (
		source tFiles
		parent tFiles
	)
	for i := 0; i < sn; i++ {
		imin := copyBytes(start)
		for j := 0; j < 2*i; j++ {
			increseKey(imin)
		}
		imax := increseKey(copyBytes(imin))
		source = append(source, &tFile{imin: makeInternalKey(nil, imin, 1, keyTypeVal), imax: makeInternalKey(nil, imax, 1, keyTypeVal)})
	}
	parent = append(parent, &tFile{imin: start, imax: source[len(source)-1].imax})
	return &compaction{
		sourceLevel: level,
		levels:      [2]tFiles{source, parent},
		imin:        source[0].imin,
		imax:        source[len(source)-1].imax,
	}
}

func TestCompactionContext(t *testing.T) {
	icmp := &iComparer{comparer.DefaultComparer}
	ctx := &compactionContext{
		sorted:   make(map[int][]*compaction),
		fifo:     make(map[int][]*compaction),
		icmp:     icmp,
		noseek:   false,
		denylist: make(map[int]struct{}),
	}
	var comps []*compaction
	comps = append(comps, testCompaction(1, 2, []byte{0x00, 0x01}))
	comps = append(comps, testCompaction(1, 2, []byte{0x00, 0x21}))
	comps = append(comps, testCompaction(1, 2, []byte{0x00, 0x11}))
	comps = append(comps, testCompaction(2, 2, []byte{0x00, 0x01}))
	comps = append(comps, testCompaction(2, 2, []byte{0x00, 0x11}))

	for _, c := range comps {
		ctx.add(c)
	}
	level1 := ctx.getSorted(1)
	for i := 0; i < len(level1)-1; i++ {
		if icmp.Compare(level1[i].imax, level1[i+1].imax) >= 0 {
			t.Fatalf("Unsorted compaction")
		}
	}
	removing := ctx.removing(1)
	if len(removing) != 6 {
		t.Fatalf("Incorrect removing file number")
	}
	recreating := ctx.recreating(1)
	if len(recreating) != 0 {
		t.Fatalf("Incorrect recreating file number")
	}
	removing = ctx.removing(2)
	if len(removing) != 4 {
		t.Fatalf("Incorrect removing file number")
	}
	recreating = ctx.recreating(2)
	if len(recreating) != 3 {
		t.Fatalf("Incorrect recreating file number")
	}
	for _, c := range comps {
		ctx.delete(c)
	}
	if ctx.count() != 0 {
		t.Fatalf("Failed to delete all compactions")
	}
}
