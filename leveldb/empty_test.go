// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import "testing"

func TestIter_Empty(t *testing.T) {
	cc := []struct {
		name string
		c    stConstructor
	}{
		{"table", &stConstructor_Table{}},
		{"memdb", &stConstructor_MemDB{}},
		{"merged", &stConstructor_MergedMemDB{}},
		{"db", &stConstructor_DB{}},
	}

	for _, p := range cc {
		c, name := p.c, p.name
		func() {
			err := c.init(t, new(stHarnessOpt))
			if err != nil {
				t.Error(name+": error when initializing constructor:", err.Error())
				return
			}
			defer c.close()
			size, err := c.finish()
			if err != nil {
				t.Error(name+": error when finishing constructor:", err.Error())
				return
			}
			t.Logf(name+": final size is %d bytes", size)
			iter := c.newIterator()
			defer iter.Release()
			if iter.Valid() {
				t.Error(name + ": Valid() return true")
			}
			if iter.Next() {
				t.Error(name + ": Next() return true")
			}
			if iter.Prev() {
				t.Error(name + ": Prev() return true")
			}
			if iter.Seek(nil) {
				t.Error(name + ": Seek(nil) return true")
			}
			if iter.Seek([]byte("v")) {
				t.Error(name + ": Seek('v') return true")
			}
			if iter.First() {
				t.Error(name + ": First() return true")
			}
			if iter.Last() {
				t.Error(name + ": Last() return true")
			}
		}()
	}
}
