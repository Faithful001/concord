package storage

import (
	"errors"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")

type Store struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

func (s *Store) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.data[key]
	if !ok {
		return ErrKeyNotFound
	}

	delete(s.data, key)
	return nil
}

// Snapshot returns a deep copy of the store's current key-value data.
func (s *Store) Snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cp := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		cp[k] = valCopy
	}
	return cp
}

// Restore resets the store's state with the provided data.
func (s *Store) Restore(data map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string][]byte, len(data))
	for k, v := range data {
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		s.data[k] = valCopy
	}
}