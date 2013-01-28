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

// Package cache provides interface and implementation of a cache algorithms.
package cache

// SetFunc used by Get method of Namespace interface as callback in the
// event of key has no mapping.
type SetFunc func() (ok bool, value interface{}, charge int, fin func())

type Cache interface {
	// Get caches namespace for given id.
	GetNamespace(id uint64) Namespace

	// Delete all caches. Note that the caches will be kept around until all
	// of its existing handles have been released and the finalizer will
	// finally be executed.
	Purge(fin func())

	// Zap all caches. Unlike Purge, Zap will delete all cache and execute
	// finalizer immediately, even if its existing handles not yet released.
	// Zap also delete namespace from namespace table, in this case emptying
	// namespace table.
	Zap()
}

type Namespace interface {
	// Get cache for given key; insert a new one if doesn't exist.
	//
	// The given setter will be called during insert operation, it may return
	// 'ok' false, which will cancel insert operation and Get will return
	// nil, false.
	Get(key uint64, setf SetFunc) (obj Object, ok bool)

	// If the cache contains entry for key, delete it.  Note that the
	// underlying entry will be kept around until all existing handles
	// to it have been released and the finalizer will finally be executed.
	Delete(key uint64, fin func()) bool

	// Delete all caches. Note that the caches will be kept around until all
	// of its existing handles have been released and the finalizer will
	// finally be executed.
	Purge(fin func())

	// Zap all caches. Unlike Purge, Zap will delete all cache and execute
	// finalizer immediately, even if its existing handles not yet released.
	// Zap also delete namespace from namespace table.
	Zap()
}

type Object interface {
	// Release the cache object.
	// REQUIRES: handle must not have been released yet.
	Release()

	// Return the value hold by cache object.
	// REQUIRES: handle must not have been released yet.
	Value() interface{}
}
