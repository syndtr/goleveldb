// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

type snapshotElement struct {
	seq uint64
	ref int
	next, prev *snapshotElement

	list *snapshotList
}

// Next returns the next list element or nil.
func (e *snapshotElement) Next() *snapshotElement {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous list element or nil.
func (e *snapshotElement) Prev() *snapshotElement {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// List represents a doubly linked list.
// The zero value for List is an empty list ready to use.
type snapshotList struct {
	root snapshotElement // sentinel list element, only &root, root.prev, and root.next are used
	len  int     // current list length excluding (this) sentinel element
}

// Init initializes or clears list l.
func (l *snapshotList) Init() *snapshotList {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

// newSnapsList returns an initialized list.
func newSnapsList() *snapshotList { return new(snapshotList).Init() }

// Len returns the number of elements of list l.
// The complexity is O(1).
func (l *snapshotList) Len() int { return l.len }

// Front returns the first element of list l or nil if the list is empty.
func (l *snapshotList) Front() *snapshotElement {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

// Back returns the last element of list l or nil if the list is empty.
func (l *snapshotList) Back() *snapshotElement {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

// lazyInit lazily initializes a zero List value.
func (l *snapshotList) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

// insert inserts e after at, increments l.len, and returns e.
func (l *snapshotList) insert(e, at *snapshotElement) {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++
	return
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (l *snapshotList) insertValue(v *snapshotElement, at *snapshotElement) {
	l.insert(v, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (l *snapshotList) remove(e *snapshotElement) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	l.len--
	return
}

// move moves e to next to at and returns e.
func (l *snapshotList) move(e, at *snapshotElement) *snapshotElement {
	if e == at {
		return e
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e

	return e
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *snapshotList) Remove(e *snapshotElement) {
	if e.list == l {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		l.remove(e)
	}
	return
}

// PushFront inserts a new element e with value v at the front of list l and returns e.
func (l *snapshotList) PushFront(v *snapshotElement) {
	l.lazyInit()
	l.insertValue(v, &l.root)
}

// PushBack inserts a new element e with value v at the back of list l and returns e.
func (l *snapshotList) PushBack(v *snapshotElement) {
	l.lazyInit()
	l.insertValue(v, l.root.prev)
}

// InsertBefore inserts a new element e with value v immediately before mark and returns e.
// If mark is not an element of l, the list is not modified.
// The mark must not be nil.
func (l *snapshotList) InsertBefore(v *snapshotElement, mark *snapshotElement) {
	if mark.list != l {
		return
	}
	// see comment in List.Remove about initialization of l
	l.insertValue(v, mark.prev)
}

// InsertAfter inserts a new element e with value v immediately after mark and returns e.
// If mark is not an element of l, the list is not modified.
// The mark must not be nil.
func (l *snapshotList) InsertAfter(v *snapshotElement, mark *snapshotElement) {
	if mark.list != l {
		return
	}
	// see comment in List.Remove about initialization of l
	l.insertValue(v, mark)
}

// MoveToFront moves element e to the front of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *snapshotList) MoveToFront(e *snapshotElement) {
	if e.list != l || l.root.next == e {
		return
	}
	// see comment in List.Remove about initialization of l
	l.move(e, &l.root)
}

// MoveToBack moves element e to the back of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *snapshotList) MoveToBack(e *snapshotElement) {
	if e.list != l || l.root.prev == e {
		return
	}
	// see comment in List.Remove about initialization of l
	l.move(e, l.root.prev)
}

// MoveBefore moves element e to its new position before mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *snapshotList) MoveBefore(e, mark *snapshotElement) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark.prev)
}

// MoveAfter moves element e to its new position after mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *snapshotList) MoveAfter(e, mark *snapshotElement) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark)
}

// PushBackList inserts a copy of another list at the back of list l.
// The lists l and other may be the same. They must not be nil.
func (l *snapshotList) PushBackList(other *snapshotList) {
	l.lazyInit()
	for i, e := other.Len(), other.Front(); i > 0; i, e = i-1, e.Next() {
		l.insertValue(e, l.root.prev)
	}
}

// PushFrontList inserts a copy of another list at the front of list l.
// The lists l and other may be the same. They must not be nil.
func (l *snapshotList) PushFrontList(other *snapshotList) {
	l.lazyInit()
	for i, e := other.Len(), other.Back(); i > 0; i, e = i-1, e.Prev() {
		l.insertValue(e, &l.root)
	}
}
