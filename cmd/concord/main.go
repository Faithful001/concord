package main

import (
	"fmt"
	"log"

	"github.com/Faithful001/concord.git/internal/storage"
)

func main() {
	store := storage.NewStore()

	if err := store.Set("foo", []byte("bar")); err != nil {
		log.Fatalf("Set failed: %v", err)
	}
	fmt.Println("Set foo = bar")

	value, err := store.Get("foo")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Get foo = %s\n", value)

	if err := store.Delete("foo"); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Println("Deleted foo")

	_, err = store.Get("foo")
	if err != nil {
		fmt.Printf("Get foo after delete: %v (expected)\n", err)
	}
}