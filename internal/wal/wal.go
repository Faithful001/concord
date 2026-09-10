// Package wal implements a minimal write-ahead log for crash recovery.
//
// # File format
//
// The WAL file is a sequence of records:
//
//	[4-byte big-endian uint32 payload length]
//	[N bytes JSON payload]
//	[4-byte big-endian uint32 CRC32 (IEEE) of the payload bytes]
//
// Any incomplete record at the end of the file (torn write on crash) is
// silently discarded during replay, so the caller must always treat the last
// record as potentially missing.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Faithful001/concord.git/pkg/raft"
)

// ── Record types ──────────────────────────────────────────────────────────────

const (
	recTypeHardState = "hs"
	recTypeEntry     = "ent"
)

// record is the JSON envelope written for every WAL record.
type record struct {
	Type     string `json:"t"`
	Term     int    `json:"term,omitempty"`
	VotedFor string `json:"voted_for,omitempty"`
	// Entry fields (only when Type == recTypeEntry)
	Index   int    `json:"index,omitempty"`
	Command []byte `json:"cmd,omitempty"`
}

// HardState is the recovered durable Raft state from the WAL.
type HardState struct {
	Term     int
	VotedFor string
}

// ── WAL ───────────────────────────────────────────────────────────────────────

// WAL is an append-only, crash-safe write-ahead log.
// All methods are goroutine-safe.
type WAL struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

// Open opens (or creates) the WAL file at <dir>/<nodeID>.wal.
// The file is opened in O_RDWR|O_APPEND|O_CREATE mode.
func Open(dir, nodeID string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, nodeID+".wal")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f}, nil
}

// AppendHardState writes a hard-state record (term, votedFor) to the WAL.
func (w *WAL) AppendHardState(term int, votedFor string) error {
	return w.appendRecord(record{
		Type:     recTypeHardState,
		Term:     term,
		VotedFor: votedFor,
	})
}

// AppendEntries writes one record per log entry to the WAL.
func (w *WAL) AppendEntries(entries []raft.LogEntry) error {
	for _, e := range entries {
		if err := w.appendRecord(record{
			Type:    recTypeEntry,
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ReadAll replays the WAL from the beginning and returns the latest HardState
// and the ordered list of log entries.  A torn record at the end of the file
// (partial write on crash) is silently ignored.
func (w *WAL) ReadAll() (*HardState, []raft.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to the beginning for replay.
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}

	var hs *HardState
	var entries []raft.LogEntry

	for {
		rec, err := readRecord(w.f)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errCRC) {
			// Torn write or end of file — stop replaying.
			break
		}
		if err != nil {
			return nil, nil, err
		}

		switch rec.Type {
		case recTypeHardState:
			if hs == nil {
				hs = &HardState{}
			}
			hs.Term = rec.Term
			hs.VotedFor = rec.VotedFor

		case recTypeEntry:
			entries = append(entries, raft.LogEntry{
				Term:    rec.Term,
				Index:   rec.Index,
				Command: rec.Command,
			})
		}
	}

	// Seek back to the end so subsequent appends go to the right place.
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, nil, err
	}

	return hs, entries, nil
}

// Truncate rewrites the WAL to contain only the given remainingEntries
// (entries after a snapshot's lastIncludedIndex) and then re-opens the file
// for appending.  Call this immediately after saving a snapshot.
func (w *WAL) Truncate(remainingEntries []raft.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tmp := w.path + ".tmp"
	ft, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	for _, e := range remainingEntries {
		rec := record{
			Type:    recTypeEntry,
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
		if err := writeRecord(ft, rec); err != nil {
			ft.Close()
			os.Remove(tmp)
			return err
		}
	}

	if err := ft.Sync(); err != nil {
		ft.Close()
		os.Remove(tmp)
		return err
	}
	ft.Close()

	// On Windows the destination file must be closed before it can be
	// replaced by os.Rename.  Close w.f now; we reopen it below.
	w.f.Close()
	w.f = nil

	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}

	// Re-open for appending.
	w.f.Close()
	w.f, err = os.OpenFile(w.path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	return err
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// ── Internal I/O helpers ──────────────────────────────────────────────────────

// errCRC is returned when a record's CRC32 does not match.
var errCRC = errors.New("wal: CRC32 mismatch")

func (w *WAL) appendRecord(r record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := writeRecord(w.f, r); err != nil {
		return err
	}
	return w.f.Sync()
}

func writeRecord(w io.Writer, r record) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}

	// [4-byte length]
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}

	// [N bytes payload]
	if _, err := w.Write(payload); err != nil {
		return err
	}

	// [4-byte CRC32]
	checksum := crc32.ChecksumIEEE(payload)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], checksum)
	_, err = w.Write(crcBuf[:])
	return err
}

func readRecord(r io.Reader) (*record, error) {
	// Read length prefix.
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err // io.EOF or io.ErrUnexpectedEOF
	}
	length := binary.BigEndian.Uint32(lenBuf[:])

	// Read payload.
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, io.ErrUnexpectedEOF
	}

	// Read CRC.
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, io.ErrUnexpectedEOF
	}

	// Verify CRC.
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(crcBuf[:]) {
		return nil, errCRC
	}

	var rec record
	if err := json.Unmarshal(payload, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}
