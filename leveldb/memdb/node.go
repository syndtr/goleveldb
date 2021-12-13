// Copyright (c) 2021, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package memdb provides in-memory key/value database implementation.
package memdb

import "encoding/binary"

// padKey pads, if necessary, the given key to be sufficiently long
// to work with the quickCmp function.
func padKey(key []byte) []byte {
	if len(key) >= 8 {
		return key
	}
	// pad
	k := make([]byte, 8)
	copy(k, key)
	return k
}

// The backing representation for the nodeData slice
type nodeInt int

// node represents a node in the skiplist. It maps directly onto the nodeData
// backing array, and is not meant to be used as a separate entity (aside for when
// creating a new one).
// Thus, it's perfectly fine if the underlying array is overly large (since the exact size
// is not known before reading the height).
// Node data is laid out as follows:
// [0]         : KV offset
// [1]         : Key length | height (8 bits)
// [2]         : Value length
// [3]         : Key first 8 bytes (padded if necessary)
// [3..height] : Next nodes
type node []nodeInt

// newNode constructs a new node. Be careful -- this allocates a new slice,
// along with space to store the next, according to the height given.
// This node later needs to be written to the backing slice, making the original
// instance moot.
func newNode(kvOffset, vLen, height int, key []byte) node {
	buf := make([]nodeInt, 4+height)
	buf[0] = nodeInt(kvOffset)
	buf[1] = nodeInt(len(key))<<8 | nodeInt(height&0xff)
	buf[2] = nodeInt(vLen)
	buf[3] = nodeInt(binary.BigEndian.Uint64(padKey(key)))
	return node(buf)
}

// kStart returns the start index for the key.
func (n node) kStart() int {
	return int(n[0])
}

// kEnd returns the start + length for the key.
func (n node) kEnd() int {
	return int(n[0] + n[1]>>8)
}

// kLen return the key length.
func (n node) kLen() int {
	return int(n[1] >> 8)
}

// vStart return the offset for the value.
func (n node) vStart() int {
	return int(n[0] + n[1]>>8)
}

// vEnd return the offset + length for value.
func (n node) vEnd() int {
	return int(n[0] + n[1]>>8 + n[2])
}

// vLen returns the value length.
func (n node) vLen() int {
	return int(n[2])
}

// setKStart sets the key offset.
func (n node) setKStart(keyOffset int) node {
	n[0] = nodeInt(keyOffset)
	return n
}

// setVLen sets the value length.
func (n node) setVLen(size int) node {
	n[2] = nodeInt(size)
	return n
}

// height return the size of the next-tower.
func (n node) height() int {
	return int(n[1] & 0xff)
}

// nextAt return the item at the given height.
func (n node) nextAt(height int) int {
	return int(n[4+height])
}

// setNextAt sets the next item at the given height
func (n node) setNextAt(height int, node int) {
	n[4+height] = nodeInt(node)
}

// nodeAt returns the node at the given index.
func (p *DB) nodeAt(idx int) node {
	return node(p.nodeData[idx:])
}

// quickCmp compares the first N bytes of the key in this  node with the first N bytes
// of the given key. If the comparison returns 0, that means a deeper comparison is
// needed.
func (n node) quickCmp(key []byte) int {
	other := binary.BigEndian.Uint64(key)
	if uint64(n[3]) < other {
		return -1
	} else if uint64(n[3]) > other {
		return 1
	}
	return 0
}
