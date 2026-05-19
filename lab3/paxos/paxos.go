package main

import (
	"fmt"
	"math/rand"
	"time"
)

// Start runs a node's event loop in a goroutine, processing messages from its inbox.
func Start(node *Node) {
	for msg := range node.inbox {
		switch msg.Type {
		case Prepare:
			handlePrepare(node, msg)
		case Promise:
			handlePromise(node, msg)
		case Accept:
			handleAccept(node, msg)
		case Accepted:
			handleAccepted(node, msg)
		case Nack:
			handleNack(node, msg)
		case Decided:
			handleDecided(node, msg)
		}
	}
}

func sendMsg(node *Node, msg Message) {
	select {
	case node.inbox <- msg:
	default:
	}
}

func broadcast(peers []*Node, msg Message) {
	for _, peer := range peers {
		sendMsg(peer, msg)
	}
}

// propose initiates a new Paxos round from this node for the given value.
func propose(node *Node, value int) {
	node.mu.Lock()
	if node.decided {
		node.mu.Unlock()
		return
	}

	// create a unique ID per proposer, per round
	node.round++
	node.proposalID = node.round*100 + node.id 
	
	node.proposedValue = value	// proposed value
	node.promises = nil			
	node.promisedFrom = make(map[int]bool)
	node.sentAccept = false
	node.acceptedCount = 0
	node.acceptedFrom = make(map[int]bool)
	node.nackCount = 0
	pid := node.proposalID
	node.mu.Unlock()

	printMu.Lock()
	fmt.Printf("[Node %d] Phase 1: sending Prepare(id=%d, value=%d)\n", node.id, pid, value)
	printMu.Unlock()

	broadcast(node.peers, Message{Type: Prepare, From: node.id, ProposalID: pid})
}

// --- Phase 1b: Acceptor receives Prepare ---
// node is the current node
// msg is the Message struct from the proposer (sender)
func handlePrepare(node *Node, msg Message) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if msg.ProposalID > node.promisedID {
		// Promise not to accept anything lower than this proposal ID
		node.promisedID = msg.ProposalID
		printMu.Lock()
		fmt.Printf("[Node %d] Promising proposal=%d from node %d\n", node.id, msg.ProposalID, msg.From)
		printMu.Unlock()
		sendMsg(node.peers[msg.From], Message{
			Type:          Promise,
			From:          node.id,
			ProposalID:    msg.ProposalID,
			AcceptedID:    node.acceptedID,
			AcceptedValue: node.acceptedValue,
			HasAccepted:   node.hasAccepted,
		})
	} else {
		// Already promised to a higher proposal — reject
		printMu.Lock()
		fmt.Printf("[Node %d] Rejecting Prepare(id=%d), already promised id=%d\n", node.id, msg.ProposalID, node.promisedID)
		printMu.Unlock()
		sendMsg(node.peers[msg.From], Message{Type: Nack, From: node.id, ProposalID: msg.ProposalID})
	}
}

// --- Phase 2a: Proposer receives Promise, sends Accept ---

func handlePromise(node *Node, msg Message) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// ignore redundant promises
	if node.decided || 							// consensus already reached
	   msg.ProposalID != node.proposalID || 	// promise is for an old round
	   node.promisedFrom[msg.From] || 			// already got a promise from this node
	   node.sentAccept { 						// already reached majority and sent accept
	
		return
	}

	node.promisedFrom[msg.From] = true			// got a promise from the msg sender node
	node.promises = append(node.promises, msg)	// keep track of number of promise messages
	
	// check if we have majority yet
	if len(node.promises) < majority {
		return
	}
	node.sentAccept = true	// once majority reached, send accept

	// if any acceptor already accepted a value, use the one with the highest accepted n.
	value := node.proposedValue
	highestAcceptedID := 0
	for _, p := range node.promises {
		if p.HasAccepted && p.AcceptedID > highestAcceptedID {		// acceptor had an n larger, use that value v
			highestAcceptedID = p.AcceptedID
			value = p.AcceptedValue
		}
	}

	printMu.Lock()
	fmt.Printf("[Node %d] Phase 2: got majority promises, sending Accept(id=%d, value=%d)\n", node.id, node.proposalID, value)
	printMu.Unlock()

	broadcast(node.peers, Message{Type: Accept, From: node.id, ProposalID: node.proposalID, Value: value})
}

// --- Phase 2b: Acceptor receives Accept ---

func handleAccept(node *Node, msg Message) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if msg.ProposalID >= node.promisedID {
		// accept the proposal and record our acceptance
		node.promisedID = msg.ProposalID	// update promised n
		node.acceptedID = msg.ProposalID	// update accepted n
		node.acceptedValue = msg.Value		// update accepted v	
		node.hasAccepted = true				// set accepted as true

		printMu.Lock()
		fmt.Printf("[Node %d] Accepted proposal=%d, value=%d\n", node.id, msg.ProposalID, msg.Value)
		printMu.Unlock()

		// notify proposer that we accepted
		sendMsg(node.peers[msg.From], Message{Type: Accepted, From: node.id, ProposalID: msg.ProposalID, Value: msg.Value})
	} else {
		// promised to something higher since Phase 1 — reject
		sendMsg(node.peers[msg.From], Message{Type: Nack, From: node.id, ProposalID: msg.ProposalID})
	}
}

// --- Proposer receives Accepted, learns consensus ---

func handleAccepted(node *Node, msg Message) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// ignore duplicates or redundant messages
	if node.decided || msg.ProposalID != node.proposalID || node.acceptedFrom[msg.From] {
		return
	}

	node.acceptedFrom[msg.From] = true	// got accept message from acceptor node
	node.acceptedCount++				// keep track of accepted count

	// majority reached, consensus reached
	if node.acceptedCount >= majority {
		node.decided = true
		node.decidedValue = msg.Value

		printMu.Lock()
		fmt.Printf("*** [Node %d] CONSENSUS REACHED: value=%d (proposal=%d) ***\n", node.id, msg.Value, node.proposalID)
		printMu.Unlock()

		// Broadcast the decided value so all nodes learn it
		broadcast(node.peers, Message{Type: Decided, From: node.id, ProposalID: node.proposalID, Value: msg.Value})
	}
}

// --- Proposer receives Nack, retries with higher ID ---

func handleNack(node *Node, msg Message) {
	node.mu.Lock()
	if node.decided || msg.ProposalID != node.proposalID {
		node.mu.Unlock()
		return
	}
	node.nackCount++
	// Retry only once we know we can't achieve majority (too many rejections)
	cannotWin := node.nackCount == nodeCount-majority+1
	value := node.proposedValue
	node.mu.Unlock()

	if cannotWin {
		delay := time.Duration(rand.Intn(200)+100) * time.Millisecond
		printMu.Lock()
		fmt.Printf("[Node %d] Cannot win proposal=%d, retrying in %v\n", node.id, msg.ProposalID, delay)
		printMu.Unlock()
		// Retry in a new goroutine so we don't block the inbox
		go func() {
			time.Sleep(delay)
			propose(node, value)
		}()
	}
}

// --- Learner receives Decided broadcast ---

func handleDecided(node *Node, msg Message) {
	node.mu.Lock()
	defer node.mu.Unlock()

	// if this node hasn't come to consensus, force it since majority was reached (learner)
	if !node.decided {
		node.decided = true
		node.decidedValue = msg.Value

		printMu.Lock()
		fmt.Printf("[Node %d] Learned decided value=%d (from node %d)\n", node.id, msg.Value, msg.From)
		printMu.Unlock()
	}
}
