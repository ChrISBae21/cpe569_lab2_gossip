package main

import (
	"fmt"
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
		nodes[i].quit = make(chan bool)

		// Init Gossip Info
		nodes[i].gossip_info.channel = make(chan HeartbeatTable, 10) // Create channel for heartbeat table passing
		nodes[i].gossip_info.hbTable = HeartbeatTable{i: {id: i, counter: 0, timestamp: calcTime(), status: true}}

		// Init Raft Info
		nodes[i].raft_info.state = follower
		nodes[i].raft_info.term = 0
		nodes[i].raft_info.votedFor = -1
		nodes[i].raft_info.channel = make(chan RaftMessage, nodeCount) // Channel for sending term from leader
	}

	for i := range nodeCount {
		// Assign neighbors for fully connected topology
		for j := range nodeCount {
			if j != i {
				nodes[i].neighbors[j] = nodes[j]
			}
		}
	}

	for i := range nodeCount {
		go StartGossip(nodes[i])
		go StartRaft(nodes[i])
	}

	zTicker := time.NewTicker(5 * time.Second)
	for i := range nodeCount {
		<-zTicker.C

		printMu.Lock()
		fmt.Printf("Node %d has died with local count: %d\n", i, nodes[i].gossip_info.hbTable[i].counter)
		printMu.Unlock()

		close(nodes[i].quit) // Kill node
	}
	zTicker.Stop()

	printMu.Lock()
	fmt.Println("ALL NODES ARE DEAD")
	printMu.Unlock()
}
