package storage

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	s := NewStore()

	if err := s.Set("foo", []byte("bar")); err != nil {
		t.Fatalf("Set returned unexpected error: %v", err)
	}

	got, err := s.Get("foo")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}

	if !bytes.Equal(got, []byte("bar")) {
		t.Errorf("Get returned %q, want %q", got, "bar")
	}
}

func TestGetMissingKey(t *testing.T) {
	s := NewStore()

	_, err := s.Get("missing")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get on missing key returned %v, want ErrKeyNotFound", err)
	}
}

func TestOverwrite(t *testing.T) {
	s := NewStore()

	if err := s.Set("foo", []byte("first")); err != nil {
		t.Fatalf("first Set returned unexpected error: %v", err)
	}
	if err := s.Set("foo", []byte("second")); err != nil {
		t.Fatalf("second Set returned unexpected error: %v", err)
	}

	got, err := s.Get("foo")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}

	if !bytes.Equal(got, []byte("second")) {
		t.Errorf("Get returned %q, want %q", got, "second")
	}
}

func TestDelete(t *testing.T) {
	s := NewStore()

	if err := s.Set("foo", []byte("bar")); err != nil {
		t.Fatalf("Set returned unexpected error: %v", err)
	}
	if err := s.Delete("foo"); err != nil {
		t.Fatalf("Delete returned unexpected error: %v", err)
	}

	_, err := s.Get("foo")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after Delete returned %v, want ErrKeyNotFound", err)
	}
}

func TestDeleteMissingKey(t *testing.T) {
	s := NewStore()

	err := s.Delete("missing")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Delete on missing key returned %v, want ErrKeyNotFound", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	store := NewStore()
	
	wg := sync.WaitGroup{}

	// concurrent writers to the same key
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func (n int){
			defer wg.Done()
			store.Set("key", []byte{byte(n)})
		}(i)
	}


	// 100 concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func ()  {
			defer wg.Done()
			store.Get("key")
		}()
	} 
	
	wg.Wait()
}

func TestSnapshotAndRestore(t *testing.T) {
	s1 := NewStore()
	s1.Set("k1", []byte("v1"))
	s1.Set("k2", []byte("v2"))

	snap := s1.Snapshot()

	// Verify deep copy by mutating original store
	s1.Set("k1", []byte("v1_mutated"))
	s1.Delete("k2")

	s2 := NewStore()
	s2.Restore(snap)

	v1, err := s2.Get("k1")
	if err != nil || !bytes.Equal(v1, []byte("v1")) {
		t.Errorf("Restore k1 = %q, want %q (err: %v)", v1, "v1", err)
	}

	v2, err := s2.Get("k2")
	if err != nil || !bytes.Equal(v2, []byte("v2")) {
		t.Errorf("Restore k2 = %q, want %q (err: %v)", v2, "v2", err)
	}
}