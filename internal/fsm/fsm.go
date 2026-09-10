// Package fsm implements the Finite State Machine that sits on top of Raft.
// It reads committed log entries from the node's applyCh, decodes them with
// the command package, and applies the resulting SET / DELETE operations to
// the underlying storage.Store.
package fsm

import (
	"log"

	"github.com/Faithful001/concord.git/internal/command"
	"github.com/Faithful001/concord.git/pkg/raft"
	"github.com/Faithful001/concord.git/internal/storage"
)

// FSM applies committed Raft log entries to the key-value store.
type FSM struct {
	store *storage.Store
}

// New returns a new FSM backed by store.
func New(store *storage.Store) *FSM {
	return &FSM{store: store}
}

// Store returns the underlying KV store (for read access by the API layer).
func (f *FSM) Store() *storage.Store {
	return f.store
}

// Snapshot captures the state of the underlying key-value store.
func (f *FSM) Snapshot() map[string][]byte {
	return f.store.Snapshot()
}

// Restore resets the state of the underlying key-value store.
func (f *FSM) Restore(data map[string][]byte) {
	f.store.Restore(data)
}


// Run reads ApplyMsgs from applyCh and applies each one.
// It returns when applyCh is closed or drained on shutdown.
func (f *FSM) Run(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		f.ApplyEntry(msg)
	}
}

// ApplyEntry applies a single committed log entry to the store.
// It can be called directly (bypassing the channel) for snapshot replay at startup.
func (f *FSM) ApplyEntry(msg raft.ApplyMsg) {
	if len(msg.Command) == 0 {
		return // no-op / heartbeat entry — nothing to apply
	}

	op, key, value, err := command.Decode(msg.Command)
	if err != nil {
		log.Printf("[fsm] decode error at index %d: %v", msg.Index, err)
		return
	}

	switch op {
	case command.OpSet:
		if err := f.store.Set(key, value); err != nil {
			log.Printf("[fsm] SET key=%q error: %v", key, err)
		}
	case command.OpDelete:
		// Ignore ErrKeyNotFound — DELETE is idempotent.
		if err := f.store.Delete(key); err != nil && err != storage.ErrKeyNotFound {
			log.Printf("[fsm] DELETE key=%q error: %v", key, err)
		}
	}
}
