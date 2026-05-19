package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type taskState struct {
	Task      Task
	State     int
	WorkerID  int
	StartTime time.Time
}

const (
	logAssign   = "ASSIGN"
	logComplete = "COMPLETE"
	logFail     = "FAIL"
	logPhase    = "PHASE"
)

type logEntry struct {
	Type     string
	TaskID   int
	TaskType string
	WorkerID int
	Phase    string
	Time     time.Time
}

type Master struct {
	mu          sync.Mutex
	mapTasks    []taskState
	reduceTasks []taskState
	workerHB    map[int]time.Time
	phase       string
	nReduce     int
	logFile     *os.File
	backupLogs  []*os.File
}

func NewMaster(files []string, nReduce int, logPath string, backupPaths []string) *Master {
	// backupPaths is a list of extra log files to replicate writes to.

	// Initialize master
	m := &Master{
		workerHB: make(map[int]time.Time),
		nReduce:  nReduce,
		phase:    "map",
	}

	nMap := len(files)

	// Create map tasks
	for i, f := range files {
		// Create one map task per file
		m.mapTasks = append(m.mapTasks, taskState{
			Task:  Task{ID: i, Type: TaskMap, InputFile: f, NReduce: nReduce, NMap: nMap},
			State: TaskIdle,
		})
	}

	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, taskState{
			Task:  Task{ID: i, Type: TaskReduce, ReduceID: i, NReduce: nReduce, NMap: nMap},
			State: TaskIdle,
		})
	}

	// Open log file
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("master: cannot open log %s: %v", logPath, err)
	}
	m.logFile = f

	// Open backup files
	for _, bp := range backupPaths {
		bf, err := os.OpenFile(bp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			// Failing to open a backup is non fatal, just skip
			log.Printf("master: warning: cannot open backup log %s: %v", bp, err)
			continue
		}
		m.backupLogs = append(m.backupLogs, bf)
		log.Printf("Master replicating log to %s", bp)
	}

	return m
}

func (m *Master) writeLog(e logEntry) {
	// Appends a log entry to primary log and all backup logs
	e.Time = time.Now()
	data, _ := json.Marshal(e)
	line := string(data) + "\n"

	fmt.Fprint(m.logFile, line)	// Write to primary log
	m.logFile.Sync()

	for _, bf := range m.backupLogs {
		// Replicate entry to each backup log
		fmt.Fprint(bf, line)
		bf.Sync()
	}
}

func (m *Master) RequestTask(workerID int) (Task, bool, bool) {
	// Called by worker to claim next available task
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.phase == "done" {
		// Job is done, worker can exit
		return Task{}, false, true
	}

	// Select possible tasks based of current phase
	tasks := m.mapTasks
	if m.phase == "reduce" {
		tasks = m.reduceTasks
	}

	// Assign idle task to worker
	for i := range tasks {
		if tasks[i].State == TaskIdle {
			// Found idle task
			tasks[i].State = TaskInProgress
			tasks[i].WorkerID = workerID
			tasks[i].StartTime = time.Now()
			m.writeLog(logEntry{
				Type:     logAssign,
				TaskID:   tasks[i].Task.ID,
				TaskType: tasks[i].Task.Type,
				WorkerID: workerID,
			})
			log.Printf("Assigned %s task %d to worker %d", tasks[i].Task.Type, tasks[i].Task.ID, workerID)
			return tasks[i].Task, true, false
		}
	}
	// No idle tasks, thus all are in progress already
	return Task{}, false, false
}

func (m *Master) ReportTask(workerID, taskID int, taskType string, success bool) {
	// Reports whether a worker has finished a task successfully or not

	m.mu.Lock()
	defer m.mu.Unlock()

	// Get reported task info
	var ts *taskState
	if taskType == TaskMap && taskID < len(m.mapTasks) {
		ts = &m.mapTasks[taskID]
	} else if taskType == TaskReduce && taskID < len(m.reduceTasks) {
		ts = &m.reduceTasks[taskID]
	}

	if ts == nil || ts.WorkerID != workerID || ts.State != TaskInProgress {
		// Ignore stale reports that may have been reassigned to another worker
		return
	}

	if success {
		ts.State = TaskCompleted
		m.writeLog(logEntry{Type: logComplete, TaskID: taskID, TaskType: taskType, WorkerID: workerID})
		log.Printf("Worker %d completed %s task %d", workerID, taskType, taskID)
		
		m.tryAdvancePhase()	// Try to move to next phase
	} else {
		// Reset task to be retried by another worker
		ts.State = TaskIdle
		ts.WorkerID = -1
		
		m.writeLog(logEntry{Type: logFail, TaskID: taskID, TaskType: taskType, WorkerID: workerID})
		log.Printf("Worker %d failed %s task %d — rescheduling", workerID, taskType, taskID)
	}
}

func (m *Master) Heartbeat(workerID int) {
	// Update worker heartbeat
	m.mu.Lock()
	m.workerHB[workerID] = time.Now()
	m.mu.Unlock()
}

func (m *Master) tryAdvancePhase() {
	// Checks if all tasks in current phase are complete before advancing to next phase
	if m.phase == "map" {
		for _, ts := range m.mapTasks {
			if ts.State != TaskCompleted {
				// Stay in map phase
				return
			}
		}

		m.phase = "reduce"	// All map tasks done, begin reduce
		m.writeLog(logEntry{Type: logPhase, Phase: "reduce"})
		log.Println("All map tasks complete — advancing to reduce phase")
	
	} else if m.phase == "reduce" {
		for _, ts := range m.reduceTasks {
			if ts.State != TaskCompleted {
				// Stay in reduce phase
				return
			}
		}

		m.phase = "done"	// All reduce tasks done, completely done
		m.writeLog(logEntry{Type: logPhase, Phase: "done"})
		log.Println("All reduce tasks complete — job finished")
	}
}

func (m *Master) checkWorkers() {
	// Checks heartbeat map for workers that have time out

	for {
		time.Sleep(HeartbeatInterval)
		m.mu.Lock()
		now := time.Now()

		// Reschedule tasks owned by timed out workers
		for id, lastHB := range m.workerHB {
			if now.Sub(lastHB) > HeartbeatTimeout {
				log.Printf("Worker %d heartbeat timeout — rescheduling its tasks", id)
				m.rescheduleWorkerTasks(id)
				delete(m.workerHB, id)
			}
		}

		// Reschedule any stuck tasks
		m.rescheduleTimedOutTasks()
		m.mu.Unlock()
	}
}

func (m *Master) rescheduleWorkerTasks(workerID int) {
	// Resets all tasks assigned to a worker and sets the worker back to idle

	// Reassign map tasks
	for i := range m.mapTasks {
		if m.mapTasks[i].WorkerID == workerID && m.mapTasks[i].State == TaskInProgress {
			m.mapTasks[i].State = TaskIdle
			m.mapTasks[i].WorkerID = -1
			log.Printf("Map task %d rescheduled (worker %d gone)", m.mapTasks[i].Task.ID, workerID)
		}
	}
	
	// Reassign reduce tasks
	for i := range m.reduceTasks {
		if m.reduceTasks[i].WorkerID == workerID && m.reduceTasks[i].State == TaskInProgress {
			m.reduceTasks[i].State = TaskIdle
			m.reduceTasks[i].WorkerID = -1
			log.Printf("Reduce task %d rescheduled (worker %d gone)", m.reduceTasks[i].Task.ID, workerID)
		}
	}
}

func (m *Master) rescheduleTimedOutTasks() {
	// Resets in progress tasks that have exceeded task timeout
	now := time.Now()

	// Reassign map tasks
	for i := range m.mapTasks {
		ts := &m.mapTasks[i]
		if ts.State == TaskInProgress && now.Sub(ts.StartTime) > TaskTimeout {
			log.Printf("Map task %d timed out — rescheduling", ts.Task.ID)
			ts.State = TaskIdle
			ts.WorkerID = -1
		}
	}

	// Reassign reduce tasks
	for i := range m.reduceTasks {
		ts := &m.reduceTasks[i]
		if ts.State == TaskInProgress && now.Sub(ts.StartTime) > TaskTimeout {
			log.Printf("Reduce task %d timed out — rescheduling", ts.Task.ID)
			ts.State = TaskIdle
			ts.WorkerID = -1
		}
	}
}

func (m *Master) IsDone() bool {
	// Returns true when all tasks done
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase == "done"
}
