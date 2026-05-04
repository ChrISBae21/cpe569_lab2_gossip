package shared

import (
	"math/rand"
	"sync"
	"time"
)

const (
	MAX_NODES = 8
)

// Node struct represents a computing node.
type Node struct {
	ID        int
	Hbcounter int
	Time      float64
	Alive     bool
}

// Generate random crash time from 10-60 seconds
func (n Node) CrashTime() int {
	rand.Seed(time.Now().UnixNano())
	max := 60
	min := 10
	return rand.Intn(max-min) + min
}

func (n Node) InitializeNeighbors(id int) [2]int {
	neighbor1 := RandInt()
	for neighbor1 == id {
		neighbor1 = RandInt()
	}
	neighbor2 := RandInt()
	for neighbor1 == neighbor2 || neighbor2 == id {
		neighbor2 = RandInt()
	}
	return [2]int{neighbor1, neighbor2}
}

func RandInt() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(MAX_NODES-1+1) + 1
}

/*---------------*/

// Membership struct represents participanting nodes
type Membership struct {
	mu      sync.RWMutex
	Members map[int]Node // map named Members with key: int, value: Node
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
	return &Membership{
		Members: make(map[int]Node),
	}
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Members[payload.ID] = payload
	*reply = payload
	return nil
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
	return m.Add(payload, reply)
}

// Returns a node with specific ID.
func (m *Membership) Get(payload int, reply *Node) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	*reply = m.Members[payload]
	return nil
}

/*---------------*/

// Request struct represents a new message request to a client
type Request struct {
	ID    int
	Table Membership
}

// Requests struct represents pending message requests
type Requests struct {
	mu      sync.Mutex
	Pending map[int]Membership
}

// Returns a new instance of a Membership (pointer).
func NewRequests() *Requests {
	return &Requests{
		Pending: make(map[int]Membership),
	}
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	// check to see if a different node already made a request to this node
	existing, ok := req.Pending[payload.ID]
	// if there is an existing request, merge them
	if ok {
		merged := CombineTables(&existing, &payload.Table)
		req.Pending[payload.ID] = *merged
	} else { // otherwise create a new request
		req.Pending[payload.ID] = payload.Table
	}
	*reply = true
	return nil
}

// Listens to communication from neighboring nodes.
func (req *Requests) Listen(ID int, reply *Membership) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	// proceed only if there is a pending request
	table, ok := req.Pending[ID]
	if ok {
		*reply = table
		delete(req.Pending, ID)
	}
	return nil
}

// combines membership tables taking the largest heartbeat per node
func CombineTables(table1 *Membership, table2 *Membership) *Membership {
	combined := NewMembership()

	table1.mu.RLock()
	for id, node := range table1.Members {
		combined.Members[id] = node
	}
	table1.mu.RUnlock()

	table2.mu.RLock()
	for id, t2_node := range table2.Members {
		t1_node, ok := combined.Members[id]
		// add if the node doesn't exist in t1's membership OR
		// if the heartbeat in t2 is more recent that t1
		if !ok || t2_node.Hbcounter > t1_node.Hbcounter {
			combined.Members[id] = t2_node
		}
	}
	table2.mu.RUnlock()

	return combined
}

