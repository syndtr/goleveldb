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

import "fmt"

type ErrorCode int

const (
	ErrorNotFound ErrorCode = iota
	ErrorCorruption
	ErrorNotImplemented
	ErrorInvalidArgument
	ErrorIO
)

var errMap = map[ErrorCode]string{
	ErrorNotFound:        "NotFound",
	ErrorCorruption:      "Corruption",
	ErrorNotImplemented:  "Not implemented",
	ErrorInvalidArgument: "Invalid argument",
	ErrorIO:              "IO error",
}

type Error struct {
	Code ErrorCode
	String string
}

func NewNotFoundError(s string) *Error {
	return &Error{Code: ErrorNotFound, String: s}
}

func NewCorruptionError(s string) *Error {
	return &Error{Code: ErrorCorruption, String: s}
}

func NewNotImplementedError(s string) *Error {
	return &Error{Code: ErrorNotImplemented, String: s}
}

func NewInvalidArgumentError(s string) *Error {
	return &Error{Code: ErrorInvalidArgument, String: s}
}

func NewIOError(s string) *Error {
	return &Error{Code: ErrorIO, String: s}
}

func (e *Error) Error() string {
	t, ok := errMap[e.Code]
	if !ok {
		return fmt.Sprintf("Unknown error(%d): %s", e.Code, e.String)
	}
	return fmt.Sprintf("%s: %s", t, e.String)
}
