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

type Cache interface {
	// Get caches namespace for given id.
	GetNamespace(id uint64) CacheNamespace

	// Delete all caches. Note that the caches will be kept around until all
	// of its existing handles have been released and the finalizer will
	// finally be executed.
	Purge(finalizer func())
}

type CacheNamespace interface {
	// Insert a mapping from key->value into the cache and assign it
	// the specified charge against the total cache capacity.
	//
	// Return a cache object that corresponds to the mapping.
	// The caller must call obj.Release() when the returned mapping is no
	// longer needed.
	Set(key uint64, value interface{}, charge int, finalizer func()) CacheObject

	// If the cache has no mapping for "key", returns nil, false.
	//
	// Else return a cache object that corresponds to the mapping.
	// The caller must call obj.Release() when the returned mapping is no
	// longer needed.
	Get(key uint64) (obj CacheObject, ok bool)

	// If the cache contains entry for key, delete it.  Note that the
	// underlying entry will be kept around until all existing handles
	// to it have been released and the finalizer will finally be executed.
	Delete(key uint64, finalizer func()) bool

	// Delete all caches. Note that the caches will be kept around until all
	// of its existing handles have been released and the finalizer will
	// finally be executed.
	Purge(finalizer func())
}

type CacheObject interface {
	// Release the cache object.
	// REQUIRES: handle must not have been released yet.
	Release()

	// Return the value hold by cache object.
	// REQUIRES: handle must not have been released yet.
	Value() interface{}
}
