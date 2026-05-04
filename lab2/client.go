package main

import (
	"fmt"
	"cpe569_lab2_gossip/shared"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
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
var self_node shared.Node

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server rpc.Client, id int, membership shared.Membership) {
	// create a new request using Request struct
	req := shared.Request{ID: id, Table: membership}
	var reply bool
	// make an rpc call to add the request
	if err := server.Call("Requests.Add", req, &reply); err != nil {
		fmt.Println("Error: Requests.Add()", err)
	}
}

// Read incoming messages from other nodes
func readMessages(server rpc.Client, id int, membership shared.Membership) *shared.Membership {
	var req shared.Membership
	// make an rpc call to get the list of requests
	if err := server.Call("Requests.Listen", id, &req); err != nil {
		fmt.Println("Error: Requests.Listen()", err)
	}
	// combine the membership list of this node and the incoming request membership list (from neighbor)
	return shared.CombineTables(&membership, &req)
}

func calcTime() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

var wg = &sync.WaitGroup{}

func main() {
	rand.Seed(time.Now().UnixNano())
	Z_TIME := rand.Intn(Z_TIME_MAX - Z_TIME_MIN) + Z_TIME_MIN

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	args := os.Args[1:]

	// Get ID from command line argument
	if len(args) == 0 {
		fmt.Println("No args given")
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Found Error", err)
	}

	fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

	currTime := calcTime()
	// Construct self
	self_node = shared.Node{ID: id, Hbcounter: 0, Time: currTime, Alive: true}
	var self_node_response shared.Node // Allocate space for a response to overwrite this

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, &self_node_response); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	neighbors := self_node.InitializeNeighbors(id)
	fmt.Println("Neighbors:", neighbors)

	membership := shared.NewMembership()
	membership.Add(self_node, &self_node)

	sendMessage(*server, neighbors[0], *membership)

	// crashTime := self_node.CrashTime()

	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, &membership, id) })
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, &membership, id) })
	time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	wg.Add(1)
	wg.Wait()
}

// checks if members are alive marks them dead after T_FAIL time
func checkAlive(membership *shared.Membership) {
	now := calcTime()
	for id, node := range membership.Members {
		if now-node.Time > T_FAIL {
			node.Alive = false
			membership.Members[id] = node
		}
	}
}

// updates own heartbeat and time and checks membership list
func runAfterX(server *rpc.Client, node *shared.Node, membership **shared.Membership, id int) {
	// update own heartbeat counter
	node.Hbcounter++
	// calculate current time
	node.Time = calcTime()
	var reply shared.Node
	// make an rpc call to update time and heartbeat
	if err := server.Call("Membership.Update", *node, &reply); err != nil {
		fmt.Println("Error: Membership.Update()", err)
	}
	// check if members in membership list are alive
	(*membership).Members[id] = *node
	checkAlive(*membership)

	// prints the heartbeat table
	fmt.Printf("Node %d heartbeat table:\n", id)
	printMembership(**membership)

	// waits for X time to update node again
	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, node, membership, id) })
}

// updates the membership table based on own neighbors and  incoming requests from other nodes
func runAfterY(server *rpc.Client, neighbors [2]int, membership **shared.Membership, id int) {
    // send membership list to neighbors
	for _, n := range neighbors {
        sendMessage(*server, n, **membership)
    }

	// update own membership table from other nodes sending to this node
    updated := readMessages(*server, id, **membership)
    if updated != nil {
        *membership = updated
    }

	// wait Y time to update membership tables
    time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })

}

// after Z time, kill the client
func runAfterZ(server *rpc.Client, id int) {
	fmt.Println("Node", id, "has crashed")
    wg.Done()
}

// prints the membership table
func printMembership(m shared.Membership){
	for _, node := range m.Members {
		status := "is Alive"
		if !node.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %.1f and %s\n", node.ID, node.Hbcounter, node.Time, status)
	}
	fmt.Println("")
}