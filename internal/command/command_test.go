package command

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeSet(t *testing.T) {
	key := "greeting"
	val := []byte("hello world")

	encoded := EncodeSet(key, val)
	op, gotKey, gotVal, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if op != OpSet {
		t.Errorf("op = 0x%02x, want 0x%02x", op, OpSet)
	}
	if gotKey != key {
		t.Errorf("key = %q, want %q", gotKey, key)
	}
	if !bytes.Equal(gotVal, val) {
		t.Errorf("val = %q, want %q", gotVal, val)
	}
}

func TestEncodeDecodeDelete(t *testing.T) {
	key := "to_delete"

	encoded := EncodeDelete(key)
	op, gotKey, gotVal, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if op != OpDelete {
		t.Errorf("op = 0x%02x, want 0x%02x", op, OpDelete)
	}
	if gotKey != key {
		t.Errorf("key = %q, want %q", gotKey, key)
	}
	if gotVal != nil {
		t.Errorf("val = %v, want nil", gotVal)
	}
}

func TestEncodeDecodeAddPeer(t *testing.T) {
	id := "node-4"
	raftAddr := "127.0.0.1:8004"
	apiAddr := "127.0.0.1:9004"

	encoded := EncodeAddPeer(id, raftAddr, apiAddr)
	op, gotID, gotRaft, gotAPI, err := DecodePeer(encoded)
	if err != nil {
		t.Fatalf("DecodePeer failed: %v", err)
	}

	if op != OpAddPeer {
		t.Errorf("op = 0x%02x, want 0x%02x", op, OpAddPeer)
	}
	if gotID != id || gotRaft != raftAddr || gotAPI != apiAddr {
		t.Errorf("DecodePeer = (%q, %q, %q), want (%q, %q, %q)", gotID, gotRaft, gotAPI, id, raftAddr, apiAddr)
	}
}

func TestEncodeDecodeRemovePeer(t *testing.T) {
	id := "node-2"

	encoded := EncodeRemovePeer(id)
	op, gotID, gotRaft, gotAPI, err := DecodePeer(encoded)
	if err != nil {
		t.Fatalf("DecodePeer failed: %v", err)
	}

	if op != OpRemovePeer {
		t.Errorf("op = 0x%02x, want 0x%02x", op, OpRemovePeer)
	}
	if gotID != id {
		t.Errorf("gotID = %q, want %q", gotID, id)
	}
	if gotRaft != "" || gotAPI != "" {
		t.Errorf("got addrs (%q, %q), want empty", gotRaft, gotAPI)
	}
}

func TestEncodeDecodeAddLearner(t *testing.T) {
	id := "node-5"
	raftAddr := "127.0.0.1:8005"
	apiAddr := "127.0.0.1:9005"

	encoded := EncodeAddLearner(id, raftAddr, apiAddr)
	op, gotID, gotRaft, gotAPI, err := DecodePeer(encoded)
	if err != nil {
		t.Fatalf("DecodePeer failed: %v", err)
	}

	if op != OpAddLearner {
		t.Errorf("op = 0x%02x, want 0x%02x", op, OpAddLearner)
	}
	if gotID != id || gotRaft != raftAddr || gotAPI != apiAddr {
		t.Errorf("DecodePeer = (%q, %q, %q), want (%q, %q, %q)", gotID, gotRaft, gotAPI, id, raftAddr, apiAddr)
	}
}
