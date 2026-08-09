package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/Faithful001/concord.git/internal/raft"
	"github.com/Faithful001/concord.git/internal/transport"
)

func main() {
	id := flag.String("id", "", "this node's ID")
	addr := flag.String("addr", "", "address to listen on, e.g. localhost:8001")
	peersFlag := flag.String("peers", "", "comma-separated id=addr pairs, e.g. node-2=localhost:8002,node-3=localhost:8003")
	flag.Parse()

	if *id == "" || *addr == "" {
		log.Fatal("both -id and -addr are required")
	}

	addresses := make(map[string]string) // id -> address
	
	var peerIDs []string
	
	if *peersFlag != "" {
		for _, pair := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(pair, "=", 2)
			addresses[parts[0]] = parts[1]
			peerIDs = append(peerIDs, parts[0])
		}
	}

	rpcTransport := transport.NewRPCTransport(addresses)
	node := raft.NewNode(*id, peerIDs, rpcTransport)

	go func() {
		fmt.Printf("[%s] listening on %s\n", *id, *addr)
		if err := transport.Serve(node, *addr); err != nil {
			log.Fatalf("serve failed: %v", err)
		}
	}()

	node.Start()

	select {} // block forever
}