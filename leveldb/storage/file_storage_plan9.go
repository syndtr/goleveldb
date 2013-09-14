// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"os"
)

type plan9FileLock struct {
	f *os.File
}

func (fl *plan9FileLock) release() error {
	return fl.f.Close()
}

func newFileLock(path string) (fl fileLock, err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return
	}
	fl = &plan9FileLock{f: f}
	return
}

func rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}
