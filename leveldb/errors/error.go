// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

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
