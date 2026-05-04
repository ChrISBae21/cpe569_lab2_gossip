package main

import "sync"

var printMu sync.Mutex

const (
	nodeCount = 5
	majority  = (nodeCount / 2) + 1
)

type MsgType int

const (
	Prepare  MsgType = iota
	Promise
	Accept
	Accepted
	Nack
	Decided
)

func (m MsgType) String() string {
	return [...]string{"Prepare", "Promise", "Accept", "Accepted", "Nack", "Decided"}[m]
}


type Message struct {
	Type          MsgType	// the type of message
	From          int		// the node ID of the sender
	ProposalID    int		// proposed n value
	Value         int		// proposed v value	
	AcceptedID    int 		// filled by acceptor in Promise: last accepted proposal n
	AcceptedValue int 		// filled by acceptor in Promise: last accepted value
	HasAccepted   bool		// whether or not accepotr accepted a value
}

// Node plays all three Paxos roles (proposer, acceptor, learner).
type Node struct {
	id    int						// id of the node
	inbox chan Message				// channel for messaging
	peers []*Node					// map of other nodes in the network
	mu    sync.Mutex				// mutex

	// Acceptor state
	promisedID    int				// highest n proposal promised to vote
	acceptedID    int				// proposal n of the last value the node accepted
	acceptedValue int				// value accepted
	hasAccepted   bool				// whether or not already accepted something

	// Proposer state
	round         int				// increments on every new proposal
	proposalID    int				// unique n to propose
	proposedValue int				// value to be proposed
	promises      []Message			// map of Promise messages collected for the round
	promisedFrom  map[int]bool 		// node IDs that Promises were received from
	sentAccept    bool				// broadcast of Accept for the round
	acceptedCount int				// how many Accept replies received
	acceptedFrom  map[int]bool		// node IDs that sent Accept messages
	nackCount     int				// how many nacks for the round

	// Learner state
	decided      bool				// whether or not decided on a value
	decidedValue int				// value consensus reached on
}
