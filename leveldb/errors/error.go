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

// Package errors implements functions to manipulate errors.
package errors

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrClosed           = ErrInvalid("database closed")
	ErrSnapshotReleased = ErrInvalid("snapshot released")
)

type ErrInvalid string

func (e ErrInvalid) Error() string {
	if e == "" {
		return "invalid argument"
	}
	return "invalid argument: " + string(e)
}

type ErrCorrupt string

func (e ErrCorrupt) Error() string {
	if e == "" {
		return "leveldb corrupted"
	}
	return "leveldb corrupted: " + string(e)
}
