package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Worker struct {
	id     int
	master *Master
	done   chan struct{}
}

func NewWorker(id int, master *Master) *Worker {
	// Initialize new worker
	return &Worker{id: id, master: master, done: make(chan struct{})}
}

func (w *Worker) run() {
	// Polls master for task and executes them
	defer close(w.done)

	// Send worker startup heartbeat to master
	w.master.Heartbeat(w.id)
	go w.sendHeartbeats()

	log.Printf("Worker %d started", w.id)

	for {
		task, hasTask, allDone := w.master.RequestTask(w.id)
		if allDone {
			// No more work, job is done
			log.Printf("Worker %d: all tasks done, shutting down", w.id)
			return
		}
		if !hasTask {
			// All tasks currently being worked on by other workers
			time.Sleep(500 * time.Millisecond)	// Wait and then query for task again
			continue
		}

		// Execute assigned task
		var success bool
		if task.Type == TaskMap {
			success = w.doMap(task)
		} else {
			success = w.doReduce(task)
		}
		w.master.ReportTask(w.id, task.ID, task.Type, success)	// Report status to master
	}
}

func (w *Worker) sendHeartbeats() {
	// Sends a heartbeat to master every heartbeat interval
	for {
		select {
		case <-w.done:
			// Worker done, stop sending heartbeat
			return
		case <-time.After(HeartbeatInterval):
			w.master.Heartbeat(w.id)
		}
	}
}

func (w *Worker) doMap(task Task) bool {
	// Executes a map task

	// Read input file
	data, err := os.ReadFile(task.InputFile)
	if err != nil {
		log.Printf("Worker %d: map task %d: cannot read %s: %v", w.id, task.ID, task.InputFile, err)
		return false
	}

	kvs := mapWords(task.InputFile, string(data))	// Get key value pairs

	// Split kv pairs into buckets for reduce
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		b := ihash(kv.Key) % task.NReduce	// partition by hashing the key
		buckets[b] = append(buckets[b], kv)
	}

	// Write each bucket to its intermediate file
	for y, bucket := range buckets {
		fname := fmt.Sprintf("mr-%d-%d", task.ID, y)
		tmp := fname + ".tmp"
		f, err := os.Create(tmp)
		if err != nil {
			log.Printf("Worker %d: map task %d: cannot create %s: %v", w.id, task.ID, fname, err)
			return false
		}

		// Encode each kv pair
		enc := json.NewEncoder(f)
		for _, kv := range bucket {
			if err := enc.Encode(kv); err != nil {
				f.Close()
				os.Remove(tmp)
				return false
			}
		}

		f.Close()
		
		// Rename file for reducer workers
		if err := os.Rename(tmp, fname); err != nil {
			return false
		}
	}

	log.Printf("Worker %d: map task %d complete (%s → %d partitions, %d pairs)",
		w.id, task.ID, task.InputFile, task.NReduce, len(kvs))
	
	return true
}

func (w *Worker) doReduce(task Task) bool {
	// Executes a reduce task

	// Collect intermediate kv pairs from map tasks
	var intermediate []KeyValue
	for mapID := 0; mapID < task.NMap; mapID++ {
		fname := fmt.Sprintf("mr-%d-%d", mapID, task.ReduceID)
		
		f, err := os.Open(fname)
		if err != nil {
			// Missing file
			continue
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// Reached EOF, move to next file
				break
			}
			intermediate = append(intermediate, kv)
		}

		f.Close()
	}

	// Sort by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// Create the output file
	oname := fmt.Sprintf("mr-out-%d", task.ReduceID)
	tmp := oname + ".tmp"
	ofile, err := os.Create(tmp)
	if err != nil {
		log.Printf("Worker %d: reduce task %d: cannot create output: %v", w.id, task.ID, err)
		return false
	}

	// Loop over groups of the same keys
	for i := 0; i < len(intermediate); {
		j := i + 1	// possible end index

		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// Collect all values for this group of keys
		vals := make([]string, j-i)
		for k := i; k < j; k++ {
			vals[k-i] = intermediate[k].Value
		}

		// Write word count to file
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, countWords(vals))
		i = j
	}

	ofile.Close()
	
	// Set to output file
	if err := os.Rename(tmp, oname); err != nil {
		return false
	}

	// Cleanup intermediate files
	for mapID := 0; mapID < task.NMap; mapID++ {
		os.Remove(fmt.Sprintf("mr-%d-%d", mapID, task.ReduceID))
	}

	log.Printf("Worker %d: reduce task %d complete (partition %d → %s)",
		w.id, task.ID, task.ReduceID, oname)
	
	return true
}

func mapWords(_ string, contents string) []KeyValue {
	// Splits file into kv pairs
	isDelim := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(contents, isDelim)

	kvs := make([]KeyValue, len(words))
	for i, w := range words {
		kvs[i] = KeyValue{w, "1"}
	}

	return kvs
}

func countWords(values []string) string {
	// Return number of occurrences
	return strconv.Itoa(len(values))
}

func ihash(key string) int {
	// Return hash for assigning keys
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
