package main

import (
	"fmt"
	"math/rand"
	"time"
)

const yFail = 4

func Start(node *Node) {
	printMu.Lock()
	fmt.Printf("Node: %d with neighbors %d, %d\n", node.id, node.neighbors[0].id, node.neighbors[1].id)
	printMu.Unlock()

	xTicker := time.NewTicker(1 * time.Second)
	yTicker := time.NewTicker(2 * time.Second)
	updateTicker := time.NewTicker(2 * time.Second)
	printTicker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-node.quit:
			xTicker.Stop()
			yTicker.Stop()
			return // Shutdown node
		case <-xTicker.C:
			node_info := node.hbTable[node.id]
			node_info.counter++ // Increment counter every x seconds

			node_info.timestamp = calcTime() // Update time of heartbeat
			node.hbTable[node.id] = node_info
		case <-yTicker.C:
			AfterYSeconds(node) // Sending over channel blocks until receiver and sender ready
		case received := <-node.channel:
			MergeTable(node, received) // Reading from the channel is blocking
		case <-updateTicker.C:
			UpdateTable(node) // Update status
		case <-printTicker.C:
			PrintTable(node)
		}
	}

}

func copyTable(table HeartbeatTable) HeartbeatTable {
	tableCopy := make(HeartbeatTable)
	for id, entry := range table {
		tableCopy[id] = entry
	}
	return tableCopy
}

func AfterYSeconds(node *Node) {
	neighbor := node.neighbors[rand.Intn(2)]
	select {
	case neighbor.channel <- copyTable(node.hbTable):
		// sent successfully
	default:
		// buffer full
	}
}

func MergeTable(node *Node, receivedTable HeartbeatTable) {
	// Update node table with new heartbeats based off of counter
	for id, received_hb := range receivedTable {
		stored_hb, exists := node.hbTable[id]
		if exists {
			// Node already in our table, need to check if it is fresher
			if received_hb.counter > stored_hb.counter {
				// Need to update heartbeat
				received_hb.timestamp = calcTime() // Update timestamp
				received_hb.status = true          // Counter incresed thus node is alive
				node.hbTable[id] = received_hb
			}
		} else {
			// Node doesnt exist in our table
			received_hb.timestamp = calcTime() // Update timestamp
			received_hb.status = true          // Decide if dead or not

			node.hbTable[id] = received_hb // Add node to our table
		}
	}
}

func UpdateTable(node *Node) {
	for id, stored_hb := range node.hbTable {
		delta_time := calcTime() - stored_hb.timestamp
		if delta_time >= yFail {
			// mark a node as dead if timestamp from current time is yFail
			stored_hb.status = false
			node.hbTable[id] = stored_hb
		}
	}
}

func PrintTable(node *Node) {
	printMu.Lock()
	defer printMu.Unlock()

	fmt.Printf("\n--- Node %d ---\n", node.id)
	fmt.Println("  ID | Counter | Timestamp     | Status")
	fmt.Println("  ---|---------|---------------|-------")
	for id, entry := range node.hbTable {
		fmt.Printf("  %d  | %d      | %.2f | ", id, entry.counter, entry.timestamp)
		if entry.status {
			fmt.Printf("Alive\n")
		} else {
			fmt.Printf("Dead\n")
		}
	}
}
