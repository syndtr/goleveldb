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

package leveldb

import (
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/filter"
	"leveldb/opt"
)

type iOptions struct {
	s *session
	o opt.OptionsGetter
}

func (o *iOptions) GetComparer() comparer.Comparer {
	return o.s.cmp
}

func (o *iOptions) HasFlag(flag opt.OptionsFlag) bool {
	return o.o.HasFlag(flag)
}

func (o *iOptions) GetWriteBuffer() int {
	return o.o.GetWriteBuffer()
}

func (o *iOptions) GetMaxOpenFiles() int {
	return o.o.GetMaxOpenFiles()
}

func (o *iOptions) GetBlockCache() cache.Cache {
	return o.o.GetBlockCache()
}

func (o *iOptions) GetBlockSize() int {
	return o.o.GetBlockSize()
}

func (o *iOptions) GetBlockRestartInterval() int {
	return o.o.GetBlockRestartInterval()
}

func (o *iOptions) GetCompressionType() opt.Compression {
	return o.o.GetCompressionType()
}

func (o *iOptions) GetFilter() filter.Filter {
	if o.s.filter == nil {
		return nil
	}
	return o.s.filter
}
