package main

import (
	"encoding/binary"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type ErrIkeyCorrupted struct {
	Ikey   []byte
	Reason string
}

func (e *ErrIkeyCorrupted) Error() string {
	return fmt.Sprintf("leveldb: iKey %q corrupted: %s", e.Ikey, e.Reason)
}

func newErrIkeyCorrupted(ikey []byte, reason string) error {
	return errors.NewErrCorrupted(storage.FileDesc{}, &ErrIkeyCorrupted{append([]byte(nil), ikey...), reason})
}

type kType int

func (kt kType) String() string {
	switch kt {
	case ktDel:
		return "d"
	case ktVal:
		return "v"
	}
	return "x"
}

// Value types encoded as the last component of internal keys.
// Don't modify; this value are saved to disk.
const (
	ktDel kType = iota
	ktVal
)

// ktSeek defines the kType that should be passed when constructing an
// internal key for seeking to a particular sequence number (since we
// sort sequence numbers in decreasing order and the value type is
// embedded as the low 8 bits in the sequence number in internal keys,
// we need to use the highest-numbered ValueType, not the lowest).
const ktSeek = ktVal

const (
	// Maximum value possible for sequence number; the 8-bits are
	// used by value type, so its can packed together in single
	// 64-bit integer.
	kMaxSeq uint64 = (uint64(1) << 56) - 1
	// Maximum value possible for packed sequence number and type.
	kMaxNum uint64 = (kMaxSeq << 8) | uint64(ktSeek)
)

// Maximum number encoded in bytes.
var kMaxNumBytes = make([]byte, 8)

func init() {
	binary.LittleEndian.PutUint64(kMaxNumBytes, kMaxNum)
}

func parseIkey(ik []byte) (ukey []byte, seq uint64, kt kType, err error) {
	if len(ik) < 8 {
		return nil, 0, 0, newErrIkeyCorrupted(ik, "invalid length")
	}
	num := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	seq, kt = num>>8, kType(num&0xff)
	if kt > ktVal {
		return nil, 0, 0, newErrIkeyCorrupted(ik, "invalid type")
	}
	ukey = ik[:len(ik)-8]
	return
}
