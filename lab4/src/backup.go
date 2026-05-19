package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
)

func RecoverFromLog(m *Master, logPath string) {
	// Replays the log to rebuild master task and phase state
	f, err := os.Open(logPath)
	if err != nil {
		// Log file doesn't exist
		return
	}
	
	defer f.Close()

	// Process each line
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var e logEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			// Skip bad entries
			log.Printf("recover: skipping malformed log entry: %v", err)
			continue
		}
		applyLogEntry(m, e)
		count++
	}

	// Reset all tasks status to idle
	for i := range m.mapTasks {
		if m.mapTasks[i].State == TaskInProgress {
			m.mapTasks[i].State = TaskIdle
			m.mapTasks[i].WorkerID = -1
		}
	}

	for i := range m.reduceTasks {
		if m.reduceTasks[i].State == TaskInProgress {
			m.reduceTasks[i].State = TaskIdle
			m.reduceTasks[i].WorkerID = -1
		}
	}

	log.Printf("Recovered %d log entries; resuming from phase=%q", count, m.phase)
}

func applyLogEntry(m *Master, e logEntry) {
	// Updates the master's state from a log

	// Phase change logs
	if e.Type == logPhase {
		m.phase = e.Phase
		return
	}

	// Find which task type the log is
	var tasks *[]taskState
	switch e.TaskType {
	case TaskMap:
		tasks = &m.mapTasks
	case TaskReduce:
		tasks = &m.reduceTasks
	default:
		return	// Unknown
	}

	// Check for bad task ID
	if e.TaskID < 0 || e.TaskID >= len(*tasks) {
		return
	}

	// Apply log entry changes
	switch e.Type {
	case logAssign:
		(*tasks)[e.TaskID].State = TaskInProgress
		(*tasks)[e.TaskID].WorkerID = e.WorkerID
	case logComplete:
		(*tasks)[e.TaskID].State = TaskCompleted
	case logFail:
		(*tasks)[e.TaskID].State = TaskIdle
		(*tasks)[e.TaskID].WorkerID = -1
	}
}
