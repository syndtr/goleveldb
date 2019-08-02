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

// osStat calls os.Stat.
func osStat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// osOpenFile calls os.OpenFile.
func osOpenFile(name string, flag int, perm os.FileMode) (osFile, error) {
	return os.OpenFile(name, flag, perm)
}

// osOpen calls os.Open.
func osOpen(name string) (osFile, error) {
	return os.Open(name)
}

// osRemove calls os.Remove.
func osRemove(name string) error {
	return os.Remove(name)
}

// Readdirnames opens the directory and calls Readdirnames on it.
func Readdirnames(dirname string, n int) (names []string, err error) {
	dir, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	return dir.Readdirnames(n)
}

// osMkdirAll calls os.MkdirAll.
func osMkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}
