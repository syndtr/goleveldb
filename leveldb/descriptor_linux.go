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
	"os"
	"syscall"
	"unsafe"
)

func setFileLock(f *os.File, lock bool) (err error) {
	ltype := syscall.F_UNLCK
	if lock {
		ltype = syscall.F_WRLCK
	}

	k := struct {
		Type   uint32
		Whence uint32
		Start  uint64
		Len    uint64
		Pid    uint32
	}{
		Type:   uint32(ltype),
		Whence: uint32(os.SEEK_SET),
		Start:  0,
		Len:    0, // lock the entire file.
		Pid:    uint32(os.Getpid()),
	}
	_, _, err = syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_SETLK), uintptr(unsafe.Pointer(&k)))
	return
}
