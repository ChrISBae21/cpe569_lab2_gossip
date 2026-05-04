package main

import (
	"sync"
)

var printMu sync.Mutex // global print mutex

const (
	msgHeartbeat    = 0
	msgRequestVote  = 1
	msgVoteResponse = 2
)

type Heartbeat struct {
	id        int
	counter   int
	timestamp float64
	status    bool
}

type HeartbeatTable map[int]Heartbeat

type GossipNode struct {
	hbTable HeartbeatTable
	channel chan HeartbeatTable
}

type RaftMessage struct {
	msgType  int
	id       int
	state    int
	term     int
	votedFor int
}

type RaftNode struct {
	state    int
	term     int
	votedFor int
	channel  chan RaftMessage
}

type Node struct {
	id          int
	neighbors   [nodeCount]*Node
	raft_info   RaftNode
	gossip_info GossipNode
	quit        chan bool
}
