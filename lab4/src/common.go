package main

import "time"

// key/value pair from the map function
type KeyValue struct {
	Key   string
	Value string
}

// Task state values
const (
	TaskIdle       = 0
	TaskInProgress = 1
	TaskCompleted  = 2
)

// Task type identifiers
const (
	TaskMap    = "map"
	TaskReduce = "reduce"
)

// Fault-detection timing values
const (
	HeartbeatInterval = 2 * time.Second
	HeartbeatTimeout  = 6 * time.Second
	TaskTimeout       = 10 * time.Second
)

type Task struct {
	ID        int
	Type      string
	InputFile string
	ReduceID  int
	NReduce   int
	NMap      int
}
