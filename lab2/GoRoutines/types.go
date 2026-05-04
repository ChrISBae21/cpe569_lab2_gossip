package main

import "sync"

var printMu sync.Mutex // global print mutex

type Heartbeat struct {
	id        int
	counter   int
	timestamp float64
	status	  bool
}

type HeartbeatTable map[int]Heartbeat

type Node struct {
	id        int
	neighbors [2]*Node
	hbTable   HeartbeatTable
	channel   chan HeartbeatTable
	quit      chan bool
}
