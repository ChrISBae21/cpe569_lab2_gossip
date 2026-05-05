package main

import (
	"fmt"
	"time"
)

func calcTime() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func main() {
	// initialize nodes each with
	nodes := make([]*Node, nodeCount)	// initialize nodes
	for i := range nodes {
		nodes[i] = &Node{
			id:           i,
			inbox:        make(chan Message, 100),
			promisedFrom: make(map[int]bool),
			acceptedFrom: make(map[int]bool),
		}
	}

	// all nodes can reach all other nodes (fully connected)
	for i := range nodes {
		nodes[i].peers = nodes
	}

	// start each node's event loop goroutine
	for i := range nodes {
		go Start(nodes[i])
	}

	// two nodes propose conflicting values concurrently
	go propose(nodes[0], 99)
	go func() {
		// slight delay so both proposals are in flight simultaneously
		// time.Sleep(50 * time.Millisecond)
		propose(nodes[2], 42)
	}()

	// wait long enough for consensus and any retries to complete
	time.Sleep(3 * time.Second)

	printMu.Lock()
	fmt.Println("\n--- Final Node States ---")
	for _, node := range nodes {
		if node.decided {
			fmt.Printf("Node %d: decided value = %d\n", node.id, node.decidedValue)
		} else {
			fmt.Printf("Node %d: no decision reached\n", node.id)
		}
	}
	printMu.Unlock()
}
