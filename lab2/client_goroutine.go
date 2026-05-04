package main

import (
	"fmt"
	"cpe569_lab2_gossip/shared"	
	"math/rand"
	"sync"
	"time"
)

const (
	MAX_NODES  = 8
	X_TIME     = 1
	Y_TIME     = 2
	Z_TIME_MAX = 100
	Z_TIME_MIN = 10
	T_FAIL     = 5.0
)

var (
	inboxes = make(map[int]chan shared.Membership)
	wg      sync.WaitGroup
)

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(id int, membership shared.Membership) {
	select {
		case inboxes[id] <- membership:
		default: // don't block if inbox is full
	}
}

// Read incoming messages and merge with current membership table
func readMessages(id int, membership shared.Membership) *shared.Membership {
	select {
		case incoming := <-inboxes[id]:
			return shared.CombineTables(&membership, &incoming)
		default:
			return &membership
	}
}

func calcTime() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func runNode(id int) {
	rand.Seed(time.Now().UnixNano())
	Z_TIME := rand.Intn(Z_TIME_MAX-Z_TIME_MIN) + Z_TIME_MIN

	fmt.Printf("Node %d will fail after %d seconds\n", id, Z_TIME)

	node := shared.Node{ID: id, Hbcounter: 0, Time: calcTime(), Alive: true}
	neighbors := node.InitializeNeighbors(id)
	fmt.Printf("Node %d neighbors: %v\n", id, neighbors)

	membership := shared.NewMembership()
	membership.Add(node, &node)

	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(&node, &membership, id) })
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(neighbors, &membership, id) })
	time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(id) })
}

func checkAlive(membership *shared.Membership) {
	now := calcTime()
	for id, node := range membership.Members {
		if now-node.Time > T_FAIL {
			node.Alive = false
			membership.Members[id] = node
		}
	}
}

func runAfterX(node *shared.Node, membership **shared.Membership, id int) {
	node.Hbcounter++
	node.Time = calcTime()
	(*membership).Members[id] = *node
	checkAlive(*membership)
	fmt.Printf("Node %d heartbeat table:\n", id)
	printMembership(**membership)
	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(node, membership, id) })
}

func runAfterY(neighbors [2]int, membership **shared.Membership, id int) {
	for _, n := range neighbors {
		sendMessage(n, **membership)
	}
	*membership = readMessages(id, **membership)
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(neighbors, membership, id) })
}

func runAfterZ(id int) {
	fmt.Printf("Node %d has crashed\n", id)
	wg.Done()
}


func printMembership(m shared.Membership) {
	for _, val := range m.Members {
		status := "is Alive"
		if !val.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %.1f and %s\n", val.ID, val.Hbcounter, val.Time, status)
	}
	fmt.Println("")
}

func main() {
	for i := 1; i <= MAX_NODES; i++ {
		inboxes[i] = make(chan shared.Membership, 10)
	}

	wg.Add(MAX_NODES)
	for i := 1; i <= MAX_NODES; i++ {
		go runNode(i)
	}
	wg.Wait()
}
