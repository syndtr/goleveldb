// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type int32o int32

func (o *int32o) acquire() {
	if atomic.AddInt32((*int32)(o), 1) != 1 {
		panic("BUG: invalid ref")
	}
}

func (o *int32o) Release() {
	if atomic.AddInt32((*int32)(o), -1) != 0 {
		panic("BUG: invalid ref")
	}
}

type releaserFunc struct {
	fn    func()
	value Value
}

func (r releaserFunc) Release() {
	if r.fn != nil {
		r.fn()
	}
}

func set(c *Cache, ns, key uint64, value Value, charge int, relf func()) *Handle {
	return c.Get(ns, key, func() (int, Value) {
		if relf != nil {
			return charge, releaserFunc{relf, value}
		}
		return charge, value
	})
}

func shuffleNodes(nodes mNodes) mNodes {
	shuffled := append(mNodes(nil), nodes...)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled
}

func generateSortedNodes(nNS, minNKey, maxNKey int) mNodes {
	var generated mNodes
	for i := 0; i < nNS; i++ {
		nKey := minNKey
		if maxNKey-minNKey > 0 {
			nKey += rand.Intn(maxNKey - minNKey)
		}
		for j := 0; j < nKey; j++ {
			generated = append(generated, &Node{ns: uint64(i), key: uint64(j)})
		}
	}
	return generated
}

func TestNodesSort(t *testing.T) {
	testFunc := func(nNS, minNKey, maxNKey int) func(t *testing.T) {
		return func(t *testing.T) {
			sorted := generateSortedNodes(nNS, minNKey, maxNKey)
			for i := 0; i < 3; i++ {
				shuffled := shuffleNodes(sorted)
				require.NotEqual(t, sorted, shuffled)
				shuffled.sort()
				require.Equal(t, sorted, shuffled)
			}
			for i, x := range sorted {
				r := sorted.search(x.ns, x.key)
				require.Equal(t, i, r)
			}
		}
	}

	t.Run("SingleNS", testFunc(1, 100, 100))
	t.Run("MultipleNS", testFunc(10, 1, 10))

	t.Run("SearchInexact", func(t *testing.T) {
		data := mNodes{
			&Node{ns: 0, key: 2},
			&Node{ns: 0, key: 3},
			&Node{ns: 0, key: 4},
			&Node{ns: 2, key: 1},
			&Node{ns: 2, key: 2},
			&Node{ns: 2, key: 3},
		}
		require.Equal(t, 0, data.search(0, 1))
		require.Equal(t, 0, data.search(0, 2))
		require.Equal(t, 3, data.search(0, 5))
		require.Equal(t, 3, data.search(1, 1001000))
		require.Equal(t, 5, data.search(2, 3))
		require.Equal(t, 6, data.search(2, 4))
		require.Equal(t, 6, data.search(10, 10))
	})
}

func TestCacheMap(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	type cacheMapTestParams struct {
		nObjects, nHandles, concurrent, repeat int
	}

	var params []cacheMapTestParams
	if testing.Short() {
		params = []cacheMapTestParams{
			{1000, 100, 20, 3},
			{10000, 300, 50, 10},
		}
	} else {
		params = []cacheMapTestParams{
			{10000, 400, 50, 3},
			{100000, 1000, 100, 10},
		}
	}

	var (
		objects [][]int32o
		handles [][]unsafe.Pointer
	)

	for _, x := range params {
		objects = append(objects, make([]int32o, x.nObjects))
		handles = append(handles, make([]unsafe.Pointer, x.nHandles))
	}

	c := NewCache(nil)

	wg := new(sync.WaitGroup)
	var done int32

	for id, param := range params {
		id := id
		param := param
		objects := objects[id]
		handles := handles[id]
		for job := 0; job < param.concurrent; job++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				for j := len(objects) * param.repeat; j >= 0; j-- {
					if t.Failed() {
						return
					}

					i := r.Intn(len(objects))
					h := c.Get(uint64(id), uint64(i), func() (int, Value) {
						o := &objects[i]
						o.acquire()
						return 1, o
					})
					if !assert.True(t, h.Value().(*int32o) == &objects[i]) {
						return
					}
					if !assert.True(t, objects[i] == 1) {
						return
					}
					if !atomic.CompareAndSwapPointer(&handles[r.Intn(len(handles))], nil, unsafe.Pointer(h)) {
						h.Release()
					}
				}
			}()
		}

		// Randomly release handles at interval.
		go func() {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			for atomic.LoadInt32(&done) == 0 {
				i := r.Intn(len(handles))
				h := (*Handle)(atomic.LoadPointer(&handles[i]))
				if h != nil && atomic.CompareAndSwapPointer(&handles[i], unsafe.Pointer(h), nil) {
					h.Release()
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Emulate constant grow-shrink.
	growShrinkStop := make(chan bool, 1)
	go func() {
		handles := make([]*Handle, 100000)
		for atomic.LoadInt32(&done) == 0 {
			for i := range handles {
				handles[i] = c.Get(999999999, uint64(i), func() (int, Value) {
					return 1, 1
				})
			}
			for _, h := range handles {
				h.Release()
			}
		}
		growShrinkStop <- true
	}()

	wg.Wait()
	atomic.StoreInt32(&done, 1)

	// Releasing handles.
	activeCount := 0
	for _, handle := range handles {
		for i := range handle {
			h := (*Handle)(atomic.LoadPointer(&handle[i]))
			if h != nil && atomic.CompareAndSwapPointer(&handle[i], unsafe.Pointer(h), nil) {
				activeCount++
				h.Release()
			}
		}
	}
	t.Logf("active_handles=%d", activeCount)

	// Checking object refs
	for id, object := range objects {
		for i, o := range object {
			require.EqualValues(t, 0, o, "invalid object ref: %d-%03d", id, i)
		}
	}

	<-growShrinkStop

	require.Zero(t, c.Nodes())
	require.Zero(t, c.Size())
	t.Logf("STATS: %#v", c.GetStats())
}

func TestCacheMap_NodesAndSize(t *testing.T) {
	c := NewCache(nil)
	require.Zero(t, c.Capacity())
	require.Zero(t, c.Nodes())
	require.Zero(t, c.Size())
	set(c, 0, 1, 1, 1, nil)
	set(c, 0, 2, 2, 2, nil)
	set(c, 1, 1, 3, 3, nil)
	set(c, 2, 1, 4, 1, nil)
	require.Equal(t, 4, c.Nodes())
	require.Equal(t, 7, c.Size())
}

func TestLRUCache_Capacity(t *testing.T) {
	c := NewCache(NewLRU(10))
	require.Equal(t, 10, c.Capacity())
	set(c, 0, 1, 1, 1, nil).Release()
	set(c, 0, 2, 2, 2, nil).Release()
	set(c, 1, 1, 3, 3, nil).Release()
	set(c, 2, 1, 4, 1, nil).Release()
	set(c, 2, 2, 5, 1, nil).Release()
	set(c, 2, 3, 6, 1, nil).Release()
	set(c, 2, 4, 7, 1, nil).Release()
	set(c, 2, 5, 8, 1, nil).Release()
	require.Equal(t, 7, c.Nodes())
	require.Equal(t, 10, c.Size())
	c.SetCapacity(9)
	require.Equal(t, 9, c.Capacity())
	require.Equal(t, 6, c.Nodes())
	require.Equal(t, 8, c.Size())
}

func TestCacheMap_NilValue(t *testing.T) {
	c := NewCache(NewLRU(10))
	h := c.Get(0, 0, func() (size int, value Value) {
		return 1, nil
	})
	require.Nil(t, h)
	require.Zero(t, c.Nodes())
	require.Zero(t, c.Size())
}

func TestLRUCache_GetLatency(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	const (
		concurrentSet = 30
		concurrentGet = 3
		duration      = 3 * time.Second
		delay         = 3 * time.Millisecond
		maxKey        = 100000
	)

	var (
		set, getHit, getAll        int32
		getMaxLatency, getDuration int64
	)

	c := NewCache(NewLRU(5000))
	wg := &sync.WaitGroup{}
	until := time.Now().Add(duration)
	for i := 0; i < concurrentSet; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for time.Now().Before(until) {
				c.Get(0, uint64(r.Intn(maxKey)), func() (int, Value) {
					time.Sleep(delay)
					atomic.AddInt32(&set, 1)
					return 1, 1
				}).Release()
			}
		}(i)
	}
	for i := 0; i < concurrentGet; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				mark := time.Now()
				if mark.Before(until) {
					h := c.Get(0, uint64(r.Intn(maxKey)), nil)
					latency := int64(time.Since(mark))
					m := atomic.LoadInt64(&getMaxLatency)
					if latency > m {
						atomic.CompareAndSwapInt64(&getMaxLatency, m, latency)
					}
					atomic.AddInt64(&getDuration, latency)
					if h != nil {
						atomic.AddInt32(&getHit, 1)
						h.Release()
					}
					atomic.AddInt32(&getAll, 1)
				} else {
					break
				}
			}
		}(i)
	}

	wg.Wait()
	getAvgLatency := time.Duration(getDuration) / time.Duration(getAll)
	t.Logf("set=%d getHit=%d getAll=%d getMaxLatency=%v getAvgLatency=%v",
		set, getHit, getAll, time.Duration(getMaxLatency), getAvgLatency)

	require.LessOrEqual(t, getAvgLatency, delay/3)
	t.Logf("STATS: %#v", c.GetStats())
}

func TestLRUCache_HitMiss(t *testing.T) {
	cases := []struct {
		key   uint64
		value string
	}{
		{1, "vvvvvvvvv"},
		{100, "v1"},
		{0, "v2"},
		{12346, "v3"},
		{777, "v4"},
		{999, "v5"},
		{7654, "v6"},
		{2, "v7"},
		{3, "v8"},
		{9, "v9"},
	}

	setFin := 0
	c := NewCache(NewLRU(1000))
	for i, x := range cases {
		set(c, 0, x.key, x.value, len(x.value), func() {
			setFin++
		}).Release()
		for j, y := range cases {
			h := c.Get(0, y.key, nil)
			if j <= i {
				// should hit
				require.NotNilf(t, h, "case '%d' iteration '%d' should hit", i, j)
				require.Equalf(t, y.value, h.Value().(releaserFunc).value, "case '%d' iteration '%d' should have valid value", i, j)
			} else {
				// should miss
				require.Nilf(t, h, "case '%d' iteration '%d' should miss", i, j)
			}
			if h != nil {
				h.Release()
			}
		}
	}

	for i, x := range cases {
		finalizerOk := false
		c.Delete(0, x.key, func() {
			finalizerOk = true
		})

		require.True(t, finalizerOk)

		for j, y := range cases {
			h := c.Get(0, y.key, nil)
			if j > i {
				// should hit
				require.NotNilf(t, h, "case '%d' iteration '%d' should hit", i, j)
				require.Equalf(t, y.value, h.Value().(releaserFunc).value, "case '%d' iteration '%d' should have valid value", i, j)
			} else {
				// should miss
				require.Nilf(t, h, "case '%d' iteration '%d' should miss", i, j)
			}
			if h != nil {
				h.Release()
			}
		}
	}

	require.Equal(t, len(cases), setFin, "some set finalizer may not be executed")
}

func TestLRUCache_Eviction(t *testing.T) {
	c := NewCache(NewLRU(12))
	o1 := set(c, 0, 1, 1, 1, nil)
	set(c, 0, 2, 2, 1, nil).Release()
	set(c, 0, 3, 3, 1, nil).Release()
	set(c, 0, 4, 4, 1, nil).Release()
	set(c, 0, 5, 5, 1, nil).Release()
	if h := c.Get(0, 2, nil); h != nil { // 1,3,4,5,2
		h.Release()
	}
	set(c, 0, 9, 9, 10, nil).Release() // 5,2,9

	for _, key := range []uint64{9, 2, 5, 1} {
		h := c.Get(0, key, nil)
		require.NotNilf(t, h, "miss for key '%d'", key)
		require.Equalf(t, int(key), h.Value(), "invalid value for key '%d'", key)
		h.Release()
	}
	o1.Release()
	for _, key := range []uint64{1, 2, 5} {
		h := c.Get(0, key, nil)
		require.NotNilf(t, h, "miss for key '%d'", key)
		require.Equalf(t, int(key), h.Value(), "invalid value for key '%d'", key)
		h.Release()
	}
	for _, key := range []uint64{3, 4, 9} {
		h := c.Get(0, key, nil)
		if !assert.Nilf(t, h, "hit for key '%d'", key) {
			require.Equalf(t, int(key), h.Value(), "invalid value for key '%d'", key)
			h.Release()
		}
	}
}

func TestLRUCache_Evict(t *testing.T) {
	lru := NewLRU(6).(*lru)
	c := NewCache(lru)
	set(c, 0, 1, 1, 1, nil).Release()
	set(c, 0, 2, 2, 1, nil).Release()
	set(c, 1, 1, 3, 1, nil).Release()
	set(c, 1, 2, 4, 1, nil).Release()
	set(c, 2, 1, 5, 1, nil).Release()
	set(c, 2, 2, 6, 1, nil).Release()

	v := 1
	for ns := 0; ns < 3; ns++ {
		for key := 1; key < 3; key++ {
			h := c.Get(uint64(ns), uint64(key), nil)
			require.NotNilf(t, h, "NS=%d key=%d", ns, key)
			require.Equal(t, v, h.Value())
			h.Release()
			v++
		}
	}

	require.True(t, c.Evict(0, 1))
	require.Equal(t, 5, lru.used)
	require.False(t, c.Evict(0, 1))

	c.EvictNS(1)
	require.Equal(t, 3, lru.used)
	require.Nil(t, c.Get(1, 1, nil))
	require.Nil(t, c.Get(1, 2, nil))

	c.EvictAll()
	require.Zero(t, lru.used)
	require.Nil(t, c.Get(0, 1, nil))
	require.Nil(t, c.Get(0, 2, nil))
	require.Nil(t, c.Get(1, 1, nil))
	require.Nil(t, c.Get(1, 2, nil))
	require.Nil(t, c.Get(2, 1, nil))
	require.Nil(t, c.Get(2, 2, nil))
}

func TestLRUCache_Delete(t *testing.T) {
	delFuncCalled := 0
	delFunc := func() {
		delFuncCalled++
	}

	c := NewCache(NewLRU(2))
	set(c, 0, 1, 1, 1, nil).Release()
	set(c, 0, 2, 2, 1, nil).Release()

	require.True(t, c.Delete(0, 1, delFunc))
	require.Nil(t, c.Get(0, 1, nil))
	require.False(t, c.Delete(0, 1, delFunc))

	h2 := c.Get(0, 2, nil)
	require.NotNil(t, h2)
	require.True(t, c.Delete(0, 2, delFunc))
	require.True(t, c.Delete(0, 2, delFunc))

	set(c, 0, 3, 3, 1, nil).Release()
	set(c, 0, 4, 4, 1, nil).Release()
	c.Get(0, 2, nil).Release()

	for key := 2; key <= 4; key++ {
		h := c.Get(0, uint64(key), nil)
		require.NotNil(t, h)
		h.Release()
	}

	h2.Release()
	require.Nil(t, c.Get(0, 2, nil))
	require.Equal(t, 4, delFuncCalled)
}

func TestLRUCache_Close(t *testing.T) {
	relFuncCalled := 0
	relFunc := func() {
		relFuncCalled++
	}
	delFuncCalled := 0
	delFunc := func() {
		delFuncCalled++
	}

	c := NewCache(NewLRU(2))
	set(c, 0, 1, 1, 1, relFunc).Release()
	set(c, 0, 2, 2, 1, relFunc).Release()

	h3 := set(c, 0, 3, 3, 1, relFunc)
	require.NotNil(t, h3)
	require.True(t, c.Delete(0, 3, delFunc))

	c.Close(true)

	require.Equal(t, 3, relFuncCalled)
	require.Equal(t, 1, delFuncCalled)
}
