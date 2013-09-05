// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

type EmptyCache struct{}

func (EmptyCache) SetCapacity(capacity int) {}

func (EmptyCache) GetNamespace(id uint64) Namespace {
	return emptyCacheNs{}
}

func (EmptyCache) Purge(fin func()) {
	if fin != nil {
		fin()
	}
}

func (EmptyCache) Zap() {}

type emptyCacheNs struct{}

func (emptyCacheNs) Get(key uint64, setf SetFunc) (Object, bool) {
	if setf == nil {
		return nil, false
	}
	if ok, value, _, fin := setf(); ok {
		return &emptyCacheObj{value, fin}, ok
	}
	return nil, false
}

func (emptyCacheNs) Delete(key uint64, fin func()) bool {
	if fin != nil {
		fin()
	}
	return false
}

func (emptyCacheNs) Purge(fin func()) {
	if fin != nil {
		fin()
	}
}

func (emptyCacheNs) Zap() {}

type emptyCacheObj struct {
	value interface{}
	fin   func()
}

func (p *emptyCacheObj) Release() {
	if p.fin != nil {
		p.fin()
	}
}

func (p *emptyCacheObj) Value() interface{} {
	return p.value
}
