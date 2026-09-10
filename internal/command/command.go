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
	OpSet        byte = 0x01
	OpDelete     byte = 0x02
	OpAddPeer    byte = 0x03
	OpRemovePeer byte = 0x04
	OpAddLearner byte = 0x05
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

// EncodeAddPeer returns the wire encoding for adding a cluster peer.
func EncodeAddPeer(id, raftAddr, apiAddr string) []byte {
	idB := []byte(id)
	raftB := []byte(raftAddr)
	apiB := []byte(apiAddr)

	buf := make([]byte, 1+4+len(idB)+4+len(raftB)+4+len(apiB))
	buf[0] = OpAddPeer
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(idB)))
	copy(buf[5:], idB)

	off := 5 + len(idB)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(raftB)))
	copy(buf[off+4:], raftB)

	off += 4 + len(raftB)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(apiB)))
	copy(buf[off+4:], apiB)

	return buf
}

// EncodeAddLearner returns the wire encoding for adding a read-only learner peer.
func EncodeAddLearner(id, raftAddr, apiAddr string) []byte {
	idB := []byte(id)
	raftB := []byte(raftAddr)
	apiB := []byte(apiAddr)

	buf := make([]byte, 1+4+len(idB)+4+len(raftB)+4+len(apiB))
	buf[0] = OpAddLearner
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(idB)))
	copy(buf[5:], idB)

	off := 5 + len(idB)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(raftB)))
	copy(buf[off+4:], raftB)

	off += 4 + len(raftB)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(apiB)))
	copy(buf[off+4:], apiB)

	return buf
}

// EncodeRemovePeer returns the wire encoding for removing a cluster peer.
func EncodeRemovePeer(id string) []byte {
	idB := []byte(id)
	buf := make([]byte, 1+4+len(idB))
	buf[0] = OpRemovePeer
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(idB)))
	copy(buf[5:], idB)
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

// DecodePeer parses an OpAddPeer, OpAddLearner, or OpRemovePeer command.
func DecodePeer(b []byte) (op byte, id, raftAddr, apiAddr string, err error) {
	if len(b) < 5 {
		return 0, "", "", "", ErrInvalidCommand
	}
	op = b[0]
	if op != OpAddPeer && op != OpAddLearner && op != OpRemovePeer {
		return 0, "", "", "", fmt.Errorf("%w: expected peer opcode, got 0x%02x", ErrInvalidCommand, op)
	}

	idLen := binary.BigEndian.Uint32(b[1:5])
	if uint32(len(b)) < 5+idLen {
		return 0, "", "", "", fmt.Errorf("%w: truncated peer id", ErrInvalidCommand)
	}
	id = string(b[5 : 5+idLen])
	rest := b[5+idLen:]

	if op == OpRemovePeer {
		return op, id, "", "", nil
	}

	// OpAddPeer and OpAddLearner have raftAddr and apiAddr
	if len(rest) < 4 {
		return 0, "", "", "", fmt.Errorf("%w: missing raftAddr length", ErrInvalidCommand)
	}
	raftLen := binary.BigEndian.Uint32(rest[:4])
	if uint32(len(rest)) < 4+raftLen {
		return 0, "", "", "", fmt.Errorf("%w: truncated raftAddr", ErrInvalidCommand)
	}
	raftAddr = string(rest[4 : 4+raftLen])
	rest = rest[4+raftLen:]

	if len(rest) < 4 {
		return 0, "", "", "", fmt.Errorf("%w: missing apiAddr length", ErrInvalidCommand)
	}
	apiLen := binary.BigEndian.Uint32(rest[:4])
	if uint32(len(rest)) < 4+apiLen {
		return 0, "", "", "", fmt.Errorf("%w: truncated apiAddr", ErrInvalidCommand)
	}
	apiAddr = string(rest[4 : 4+apiLen])

	return op, id, raftAddr, apiAddr, nil
}

