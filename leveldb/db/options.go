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

package db

import "leveldb"

type iOptions struct {
	s   *session
	opt leveldb.OptionsInterface
}

func (o *iOptions) GetComparer() leveldb.Comparer {
	return o.s.cmp
}

func (o *iOptions) HasFlag(flag leveldb.OptionsFlag) bool {
	return o.opt.HasFlag(flag)
}

func (o *iOptions) GetWriteBuffer() int {
	return o.opt.GetWriteBuffer()
}

func (o *iOptions) GetMaxOpenFiles() int {
	return o.opt.GetMaxOpenFiles()
}

func (o *iOptions) GetBlockCache() leveldb.Cache {
	return o.opt.GetBlockCache()
}

func (o *iOptions) GetBlockSize() int {
	return o.opt.GetBlockSize()
}

func (o *iOptions) GetBlockRestartInterval() int {
	return o.opt.GetBlockRestartInterval()
}

func (o *iOptions) GetCompressionType() leveldb.Compression {
	return o.opt.GetCompressionType()
}

func (o *iOptions) GetFilter() leveldb.Filter {
	if o.s.filter == nil {
		return nil
	}
	return o.s.filter
}
