// Package command encodes and decodes the opaque Command []byte stored inside
// each Raft LogEntry.  The Raft layer never interprets these bytes; the FSM
// decodes them to decide what to apply to storage.Store.
//
// Wire format
//
//	SET:    [0x01][key_len uint32 BE][key bytes][val_len uint32 BE][val bytes]
//	DELETE: [0x02][key_len uint32 BE][key bytes]
package command

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	OpSet    byte = 0x01
	OpDelete byte = 0x02
)

// ErrInvalidCommand is returned when Decode receives malformed bytes.
var ErrInvalidCommand = errors.New("command: invalid bytes")

// EncodeSet returns the wire encoding for a SET k=v operation.
func EncodeSet(key string, value []byte) []byte {
	kb := []byte(key)
	// for the size param - 1: OpCode, 4: key length, len(kb): key bytes, 4: value length, len(value): value bytes
	buf := make([]byte, 1+4+len(kb)+4+len(value))
	buf[0] = OpSet
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(kb)))
	copy(buf[5:], kb)
	off := 5 + len(kb)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(value)))
	copy(buf[off+4:], value)
	return buf
}

// EncodeDelete returns the wire encoding for a DELETE k operation.
func EncodeDelete(key string) []byte {
	kb := []byte(key)
	// for the size param - 1: OpCode, 4: key length, len(kb): key bytes
	buf := make([]byte, 1+4+len(kb))
	buf[0] = OpDelete
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(kb)))
	copy(buf[5:], kb)
	return buf
}

// Decode parses a command produced by EncodeSet or EncodeDelete.
func Decode(b []byte) (op byte, key string, value []byte, err error) {
	if len(b) < 5 {
		return 0, "", nil, ErrInvalidCommand
	}
	op = b[0]
	keyLen := binary.BigEndian.Uint32(b[1:5])
	if uint32(len(b)) < 5+keyLen {
		return 0, "", nil, fmt.Errorf("%w: truncated key", ErrInvalidCommand)
	}
	key = string(b[5 : 5+keyLen])
	rest := b[5+keyLen:]

	switch op {
	case OpSet:
		if len(rest) < 4 {
			return 0, "", nil, fmt.Errorf("%w: missing value length", ErrInvalidCommand)
		}
		valLen := binary.BigEndian.Uint32(rest[:4])
		if uint32(len(rest)) < 4+valLen {
			return 0, "", nil, fmt.Errorf("%w: truncated value", ErrInvalidCommand)
		}
		value = rest[4 : 4+valLen]
	case OpDelete:
		// no value field for DELETE
	default:
		return 0, "", nil, fmt.Errorf("%w: unknown opcode 0x%02x", ErrInvalidCommand, op)
	}
	return op, key, value, nil
}
