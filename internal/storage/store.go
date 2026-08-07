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