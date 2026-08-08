package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Faithful001/concord.git/internal/raft"
)

func main() {
	// create the mock nodes
	peerIds := []string{"node-1", "node-2", "node-3"}

	// instantiate the transport
	transport := raft.NewMockTransport()

	nodes := make(map[string]*raft.Node)

	for _, id := range peerIds {
		nodes[id] = raft.NewNode(id, otherIds(peerIds, id), transport)
	}
	

	// register each node
	for id, node := range nodes {
		if err := transport.Register(id, node); err != nil {
			log.Fatalf("failed to register node %s: %v", id, err)
		}

		log.Printf("node %s registered", id)
	}

	// start each node
	for id, node := range nodes {
		node.Start()
		log.Printf("node %s started", id)
	}

	// wait for 10 seconds
	time.Sleep(10 * time.Second)

	fmt.Println("stopping cluster")
	for _, node := range nodes {
		node.Stop()
	}
}

func otherIds(allIds []string, excludeId string) []string {
	var result []string
	
	for _, id := range allIds {
		if id != excludeId {
			result = append(result, id)
		}	
	}

	return result
}