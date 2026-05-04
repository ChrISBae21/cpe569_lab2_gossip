package main

import (
	"fmt"
	"math/rand"
	"time"
)

const nodeCount = 8

func calcTime() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func main() {
	nodes := [nodeCount]*Node{} // Initialize computing nodes with zero value

	for i := range nodeCount {
		// Initialize nodes
		nodes[i] = &Node{}

		nodes[i].id = i
		nodes[i].channel = make(chan HeartbeatTable, 10) // Create channel for heartbeat table passing
		nodes[i].quit = make(chan bool)
		nodes[i].hbTable = HeartbeatTable{i: {id: i, counter: 0, timestamp: calcTime(), status: true}}

	}

	for i := range nodeCount {
		// Assign neighbors randomly
		neighbor1 := rand.Intn(nodeCount)
		for neighbor1 == i {
			neighbor1 = rand.Intn(nodeCount)
		}
		neighbor2 := rand.Intn(nodeCount)
		for neighbor1 == neighbor2 || neighbor2 == i {
			neighbor2 = rand.Intn(nodeCount)
		}

		nodes[i].neighbors[0] = nodes[neighbor1]
		nodes[i].neighbors[1] = nodes[neighbor2]

	}

	for i := range nodeCount {
		go Start(nodes[i]) // Start gossip protocol
	}

	zTicker := time.NewTicker(10 * time.Second)
	for i := range nodeCount {
		<-zTicker.C

		printMu.Lock()
		fmt.Printf("Node %d has died with local count: %d\n", i, nodes[i].hbTable[i].counter)
		printMu.Unlock()

		nodes[i].quit <- true // Kill node
	}
	zTicker.Stop()

	printMu.Lock()
	fmt.Println("ALL NODES ARE DEAD")
	printMu.Unlock()
}
