// +build !js

// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reservefs.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"os"
)

func OSStat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func OSOpenFile(name string, flag int, perm os.FileMode) (OSFile, error) {
	return os.OpenFile(name, flag, perm)
}

func OSOpen(name string) (OSFile, error) {
	return os.Open(name)
}

func OSRemove(name string) error {
	return os.Remove(name)
}

func Readdirnames(path string, n int) (names []string, err error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return dir.Readdirnames(n)
}

func MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}
