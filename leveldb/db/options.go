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
	s *session
}

func (o *iOptions) GetComparer() leveldb.Comparer {
	return o.s.icmp
}

func (o *iOptions) HasFlag(flag leveldb.OptionsFlag) bool {
	return o.s.opt.HasFlag(flag)
}

func (o *iOptions) GetWriteBuffer() int {
	return o.s.opt.GetWriteBuffer()
}

func (o *iOptions) GetMaxOpenFiles() int {
	return o.s.opt.GetMaxOpenFiles()
}

func (o *iOptions) GetBlockCache() leveldb.Cache {
	return o.s.opt.GetBlockCache()
}

func (o *iOptions) GetBlockSize() int {
	return o.s.opt.GetBlockSize()
}

func (o *iOptions) GetBlockRestartInterval() int {
	return o.s.opt.GetBlockRestartInterval()
}

func (o *iOptions) GetCompressionType() leveldb.Compression {
	return o.s.opt.GetCompressionType()
}

func (o *iOptions) GetFilter() leveldb.Filter {
	if o.s.ifilter == nil {
		return nil
	}
	return o.s.ifilter
}
