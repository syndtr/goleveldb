// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"code.google.com/p/snappy-go/snappy"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	ErrNotFound     = util.ErrNotFound
	ErrIterReleased = errors.New("leveldb/table: iterator released")
)

type block struct {
	cmp            comparer.BasicComparer
	data           []byte
	restartsLen    int
	restartsOffset int
	// Whether checksum is verified and valid.
	checksum bool
}

func (b *block) seek(key []byte) (offset int, err error) {
	if len(key) > 0 {
		n := b.restartsOffset
		data := b.data
		cmp := b.cmp
		index := sort.Search(b.restartsLen, func(i int) bool {
			o := int(binary.LittleEndian.Uint32(data[n+4*i:]))
			o++
			v1, n1 := binary.Uvarint(data[o:])
			_, n2 := binary.Uvarint(data[o+n1:])
			m := o + n1 + n2
			s := data[m : m+int(v1)]
			return cmp.Compare(s, key) > 0
		})
		if index > 0 {
			offset = int(binary.LittleEndian.Uint32(data[n+4*(index-1):]))
		}
	}
	return
}

func (b *block) restartIndex(start, offset int) int {
	n := b.restartsOffset
	data := b.data
	return sort.Search(b.restartsLen-start, func(i int) bool {
		o := int(binary.LittleEndian.Uint32(data[n+4*(start+i):]))
		return offset <= o
	}) + start - 1
}

func (b *block) restartOffset(index int) int {
	return int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*index:]))
}

func (b *block) entry(offset int) (key, value []byte, nShared, n int, err error) {
	if offset >= b.restartsOffset {
		if offset != b.restartsOffset {
			err = errors.New("leveldb/table: Reader: invalid block (block entries offset not aligned)")
		}
		return
	}
	v0, n0 := binary.Uvarint(b.data[offset:])
	v1, n1 := binary.Uvarint(b.data[offset+n0:])
	v2, n2 := binary.Uvarint(b.data[offset+n0+n1:])
	m := n0 + n1 + n2
	n = m + int(v1) + int(v2)
	if n0 <= 0 || n1 <= 0 || n2 <= 0 || offset+n > b.restartsOffset {
		err = errors.New("leveldb/table: Reader: invalid block (block entries corrupted)")
		return
	}
	key = b.data[offset+m : offset+m+int(v1)]
	value = b.data[offset+m+int(v1) : offset+n]
	nShared = int(v0)
	return
}

func (b *block) newIterator(cache util.Releaser) *blockIter {
	return &blockIter{
		block: b,
		cache: cache,
		// Valid key should never be nil.
		key: make([]byte, 0),
		dir: dirSOI,
	}
}

type dir int

const (
	dirReleased dir = iota - 1
	dirSOI
	dirEOI
	dirBackward
	dirForward
)

type blockIter struct {
	block           *block
	cache, releaser util.Releaser
	key, value      []byte
	offset          int
	// Previous offset, only filled by Next.
	prevOffset   int
	prevNode     []int
	prevKeys     []byte
	restartIndex int
	// Iterator direction.
	dir dir
	err error
}

func (i *blockIter) sErr(err error) {
	i.err = err
	i.key = nil
	i.value = nil
	i.prevNode = nil
	i.prevKeys = nil
}

func (i *blockIter) First() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	if i.dir == dirBackward {
		i.prevNode = i.prevNode[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	i.offset = 0
	i.dir = dirSOI
	return i.Next()
}

func (i *blockIter) Last() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	if i.dir == dirBackward {
		i.prevNode = i.prevNode[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	i.offset = i.block.restartsOffset
	i.dir = dirEOI
	return i.Prev()
}

func (i *blockIter) Seek(key []byte) bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	offset, err := i.block.seek(key)
	if err != nil {
		i.sErr(err)
		return false
	}
	if i.dir == dirEOI {
		i.dir = dirForward
	}
	i.offset = offset
	cmp := i.block.cmp
	for i.Next() && cmp.Compare(i.key, key) < 0 {
	}
	return i.err == nil && i.dir == dirForward
}

func (i *blockIter) Next() bool {
	if i.dir == dirEOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	if i.dir == dirBackward {
		i.prevNode = i.prevNode[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	key, value, nShared, n, err := i.block.entry(i.offset)
	if err != nil {
		i.sErr(err)
		return false
	}
	if n == 0 {
		i.dir = dirEOI
		i.key = i.key[:0]
		i.value = i.value[:0]
		return false
	}
	i.key = append(i.key[:nShared], key...)
	i.value = value
	i.prevOffset = i.offset
	i.offset += n
	i.dir = dirForward
	return true
}

func (i *blockIter) Prev() bool {
	if i.dir == dirSOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	var restartIndex int
	if i.dir == dirForward {
		// Change direction.
		i.offset = i.prevOffset
		if i.offset == 0 {
			i.dir = dirSOI
			i.key = i.key[:0]
			i.value = i.value[:0]
			return false
		}
		restartIndex = i.block.restartIndex(0, i.offset)
		i.dir = dirBackward
	} else if i.dir == dirEOI {
		// At the end of iterator.
		restartIndex = i.block.restartsLen - 1
		i.dir = dirBackward
	} else if len(i.prevNode) == 1 {
		// This is the end of a restart range.
		i.offset = i.prevNode[0]
		i.prevNode = i.prevNode[:0]
		if i.restartIndex == 0 {
			i.dir = dirSOI
			i.key = i.key[:0]
			i.value = i.value[:0]
			return false
		}
		i.restartIndex--
		restartIndex = i.restartIndex
	} else {
		// In the middle of restart range, get from cache.
		n := len(i.prevNode) - 3
		node := i.prevNode[n:]
		i.prevNode = i.prevNode[:n]
		// Get the key.
		ko := node[0]
		i.key = append(i.key[:0], i.prevKeys[ko:]...)
		i.prevKeys = i.prevKeys[:ko]
		// Get the value.
		vo := node[1]
		vl := vo + node[2]
		i.value = i.block.data[vo:vl]
		i.offset = vl
		return true
	}
	// Build entries cache.
	i.key = i.key[:0]
	i.value = nil
	offset := i.block.restartOffset(restartIndex)
	if offset == i.offset {
		restartIndex--
		if restartIndex < 0 {
			i.dir = dirSOI
			return false
		}
		offset = i.block.restartOffset(restartIndex)
	}
	i.prevNode = append(i.prevNode, offset)
	for {
		key, value, nShared, n, err := i.block.entry(offset)
		if err != nil {
			i.sErr(err)
			return false
		}
		if i.value != nil {
			// Appends 3 variables:
			// 1. Previous keys offset
			// 2. Value offset in the data block
			// 3. Value length
			i.prevNode = append(i.prevNode, len(i.prevKeys), offset-len(i.value), len(i.value))
			i.prevKeys = append(i.prevKeys, i.key...)
		}
		i.key = append(i.key[:nShared], key...)
		i.value = value
		offset += n
		// Stop if target offset reached.
		if offset >= i.offset {
			if offset != i.offset {
				i.sErr(errors.New("leveldb/table: Reader: invalid block (block entries offset not aligned)"))
				return false
			}

			break
		}
	}
	i.restartIndex = restartIndex
	i.offset = offset
	return true
}

func (i *blockIter) Key() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.key
}

func (i *blockIter) Value() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.value
}

func (i *blockIter) Release() {
	i.prevNode = nil
	i.prevKeys = nil
	i.key = nil
	i.value = nil
	i.dir = dirReleased
	if i.cache != nil {
		i.cache.Release()
		i.cache = nil
	}
	if i.releaser != nil {
		i.releaser.Release()
		i.releaser = nil
	}
}

func (i *blockIter) SetReleaser(releaser util.Releaser) {
	if i.dir > dirReleased {
		i.releaser = releaser
	}
}

func (i *blockIter) Valid() bool {
	return i.err == nil && (i.dir == dirBackward || i.dir == dirForward)
}

func (i *blockIter) Error() error {
	return i.err
}

type filterBlock struct {
	filter     filter.Filter
	data       []byte
	oOffset    int
	baseLg     uint
	filtersNum int
}

func (b *filterBlock) contains(offset uint64, key []byte) bool {
	i := int(offset >> b.baseLg)
	if i < b.filtersNum {
		o := b.data[b.oOffset+i*4:]
		n := int(binary.LittleEndian.Uint32(o))
		m := int(binary.LittleEndian.Uint32(o[4:]))
		if n < m && m <= b.oOffset {
			return b.filter.Contains(b.data[n:m], key)
		} else if n == m {
			return false
		}
	}
	return true
}

type indexIter struct {
	blockIter
	tableReader *Reader
	// Options
	checksum  bool
	fillCache bool
}

func (i *indexIter) Get() iterator.Iterator {
	value := i.Value()
	if value == nil {
		return nil
	}
	dataBH, n := decodeBlockHandle(value)
	if n == 0 {
		return iterator.NewEmptyIterator(errors.New("leveldb/table: Reader: invalid table (bad data block handle)"))
	}
	iter := i.tableReader.getDataIter(dataBH, i.checksum, i.fillCache)
	return iter
}

// Reader is a table reader.
type Reader struct {
	reader io.ReaderAt
	cache  cache.Namespace
	err    error
	// Options
	cmp        comparer.Comparer
	filter     filter.Filter
	checksum   bool
	strictIter bool

	dataEnd     int64
	indexBlock  *block
	filterBlock *filterBlock
}

func verifyChecksum(data []byte) bool {
	n := len(data) - 4
	checksum0 := binary.LittleEndian.Uint32(data[n:])
	checksum1 := util.NewCRC(data[:n]).Value()
	return checksum0 == checksum1
}

func (r *Reader) readRawBlock(bh blockHandle, checksum bool) ([]byte, error) {
	data := make([]byte, bh.length+blockTrailerLen)
	if _, err := r.reader.ReadAt(data, int64(bh.offset)); err != nil && err != io.EOF {
		return nil, err
	}
	if checksum || r.checksum {
		if !verifyChecksum(data) {
			return nil, errors.New("leveldb/table: Reader: invalid block (checksum mismatch)")
		}
	}
	switch data[bh.length] {
	case blockTypeNoCompression:
		data = data[:bh.length]
	case blockTypeSnappyCompression:
		var err error
		data, err = snappy.Decode(nil, data[:bh.length])
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("leveldb/table: Reader: unknown block compression type: %d", data[bh.length])
	}
	return data, nil
}

func (r *Reader) readBlock(bh blockHandle, checksum bool) (*block, error) {
	data, err := r.readRawBlock(bh, checksum)
	if err != nil {
		return nil, err
	}
	restartsLen := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	b := &block{
		cmp:            r.cmp,
		data:           data,
		restartsLen:    restartsLen,
		restartsOffset: len(data) - (restartsLen+1)*4,
		checksum:       checksum || r.checksum,
	}
	return b, nil
}

func (r *Reader) readFilterBlock(bh blockHandle, filter filter.Filter) (*filterBlock, error) {
	data, err := r.readRawBlock(bh, true)
	if err != nil {
		return nil, err
	}
	n := len(data)
	if n < 5 {
		return nil, errors.New("leveldb/table: Reader: invalid filter block (too short)")
	}
	m := n - 5
	oOffset := int(binary.LittleEndian.Uint32(data[m:]))
	if oOffset > m {
		return nil, errors.New("leveldb/table: Reader: invalid filter block (invalid offset)")
	}
	b := &filterBlock{
		filter:     filter,
		data:       data,
		oOffset:    oOffset,
		baseLg:     uint(data[n-1]),
		filtersNum: (m - oOffset) / 4,
	}
	return b, nil
}

func (r *Reader) getDataIter(dataBH blockHandle, checksum, fillCache bool) iterator.Iterator {
	if r.cache != nil {
		// Get/set block cache.
		var err error
		cache, ok := r.cache.Get(dataBH.offset, func() (ok bool, value interface{}, charge int, fin cache.SetFin) {
			if !fillCache {
				return
			}
			var dataBlock *block
			dataBlock, err = r.readBlock(dataBH, checksum)
			if err == nil {
				ok = true
				value = dataBlock
				charge = int(dataBH.length)
			}
			return
		})
		if err != nil {
			return iterator.NewEmptyIterator(err)
		}
		if ok {
			dataBlock := cache.Value().(*block)
			if !dataBlock.checksum && (r.checksum || checksum) {
				if !verifyChecksum(dataBlock.data) {
					return iterator.NewEmptyIterator(errors.New("leveldb/table: Reader: invalid block (checksum mismatch)"))
				}
				dataBlock.checksum = true
			}
			iter := dataBlock.newIterator(cache)
			return iter
		}
	}
	dataBlock, err := r.readBlock(dataBH, checksum)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	iter := dataBlock.newIterator(nil)
	return iter
}

// NewIterator returns an iterator of the table.
//
// The returned iterator is not goroutine-safe and should be released
// when not used.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (r *Reader) NewIterator(ro *opt.ReadOptions) iterator.Iterator {
	if r.err != nil {
		return iterator.NewEmptyIterator(r.err)
	}

	index := &indexIter{
		blockIter:   *r.indexBlock.newIterator(nil),
		tableReader: r,
		checksum:    ro.GetStrict(opt.StrictBlockChecksum),
		fillCache:   !ro.GetDontFillCache(),
	}
	return iterator.NewIndexedIterator(index, r.strictIter || ro.GetStrict(opt.StrictIterator))
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Find returns.
func (r *Reader) Find(key []byte, ro *opt.ReadOptions) (rkey, value []byte, err error) {
	if r.err != nil {
		err = r.err
		return
	}

	index := r.indexBlock.newIterator(nil)
	defer index.Release()
	if !index.Seek(key) {
		err = index.Error()
		if err == nil {
			err = ErrNotFound
		}
		return
	}
	dataBH, n := decodeBlockHandle(index.Value())
	if n == 0 {
		err = errors.New("leveldb/table: Reader: invalid table (bad data block handle)")
		return
	}
	if r.filterBlock != nil && !r.filterBlock.contains(dataBH.offset, key) {
		err = ErrNotFound
		return
	}
	data := r.getDataIter(dataBH, ro.GetStrict(opt.StrictBlockChecksum), !ro.GetDontFillCache())
	defer data.Release()
	if !data.Seek(key) {
		err = data.Error()
		if err == nil {
			err = ErrNotFound
		}
		return
	}
	rkey = data.Key()
	value = data.Value()
	return
}

// Get gets the value for the given key. It returns errors.ErrNotFound
// if the table does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (r *Reader) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	if r.err != nil {
		err = r.err
		return
	}

	rkey, value, err := r.Find(key, ro)
	if err == nil && r.cmp.Compare(rkey, key) != 0 {
		value = nil
		err = ErrNotFound
	}
	return
}

// GetApproximateOffset returns approximate offset for the given key.
//
// It is safe to modify the contents of the argument after Get returns.
func (r *Reader) GetApproximateOffset(key []byte) (offset int64, err error) {
	if r.err != nil {
		err = r.err
		return
	}

	index := r.indexBlock.newIterator(nil)
	defer index.Release()
	if index.Seek(key) {
		dataBH, n := decodeBlockHandle(index.Value())
		if n == 0 {
			err = errors.New("leveldb/table: Reader: invalid table (bad data block handle)")
			return
		}
		offset = int64(dataBH.offset)
		return
	}
	err = index.Error()
	if err == nil {
		offset = r.dataEnd
	}
	return
}

// NewReader creates a new initialized table reader for the file.
// The cache is optional and can be nil.
func NewReader(f io.ReaderAt, size int64, cache cache.Namespace, o *opt.Options) *Reader {
	r := &Reader{
		reader:     f,
		cache:      cache,
		cmp:        o.GetComparer(),
		checksum:   o.GetStrict(opt.StrictBlockChecksum),
		strictIter: o.GetStrict(opt.StrictIterator),
	}
	if f == nil {
		r.err = errors.New("leveldb/table: Reader: nil file")
		return r
	}
	if size < footerLen {
		r.err = errors.New("leveldb/table: Reader: invalid table (file size is too small)")
		return r
	}
	var footer [footerLen]byte
	if _, err := r.reader.ReadAt(footer[:], size-footerLen); err != nil && err != io.EOF {
		r.err = fmt.Errorf("leveldb/table: Reader: invalid table (could not read footer): %v", err)
	}
	if string(footer[footerLen-len(magic):footerLen]) != magic {
		r.err = errors.New("leveldb/table: Reader: invalid table (bad magic number)")
		return r
	}
	// Decode the metaindex block handle.
	metaBH, n := decodeBlockHandle(footer[:])
	if n == 0 {
		r.err = errors.New("leveldb/table: Reader: invalid table (bad metaindex block handle)")
		return r
	}
	// Decode the index block handle.
	indexBH, n := decodeBlockHandle(footer[n:])
	if n == 0 {
		r.err = errors.New("leveldb/table: Reader: invalid table (bad index block handle)")
		return r
	}
	// Read index block.
	r.indexBlock, r.err = r.readBlock(indexBH, true)
	if r.err != nil {
		return r
	}
	// Read metaindex block.
	metaBlock, err := r.readBlock(metaBH, true)
	if err != nil {
		r.err = err
		return r
	}
	// Set data end.
	r.dataEnd = int64(metaBH.offset)
	metaIter := metaBlock.newIterator(nil)
	for metaIter.Next() {
		key := string(metaIter.Key())
		if !strings.HasPrefix(key, "filter.") {
			continue
		}
		fn := key[7:]
		var filter filter.Filter
		if f0 := o.GetFilter(); f0 != nil && f0.Name() == fn {
			filter = f0
		} else {
			for _, f0 := range o.GetAltFilters() {
				if f0.Name() == fn {
					filter = f0
					break
				}
			}
		}
		if filter != nil {
			filterBH, n := decodeBlockHandle(metaIter.Value())
			if n == 0 {
				continue
			}
			// Update data end.
			r.dataEnd = int64(filterBH.offset)
			filterBlock, err := r.readFilterBlock(filterBH, filter)
			if err != nil {
				continue
			}
			r.filterBlock = filterBlock
			break
		}
	}
	metaIter.Release()
	return r
}
