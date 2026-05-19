package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Set directory and file paths
const (
	outputDir = "../output"
	logsDir   = "../logs"
	masterLog = logsDir + "/master.log"
	finalOut  = outputDir + "/mr-out-final"
)

func main() {
	// Process program args
	// Should be formatted to "go run . [-backup a.log,b.log] <nworkers> <nreduce> <input-files>"
	backupFlag := flag.String("backup", "", "comma-separated backup log paths inside logs/ (optional)")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 3 {
		usage()
		os.Exit(1)
	}

	// Convert to correct types
	nWorkers := mustAtoi(args[0])
	nReduce := mustAtoi(args[1])
	files := args[2:]

	// Get absolute backup log paths
	var backups []string
	if *backupFlag != "" {
		for _, b := range strings.Split(*backupFlag, ",") {
			backups = append(backups, filepath.Join(logsDir, b))
		}
	}

	// Check directories exist
	os.MkdirAll(outputDir, 0755)
	os.MkdirAll(logsDir, 0755)
	cleanup()	// Start on fresh directories

	m := NewMaster(files, nReduce, masterLog, backups)	// Create master
	RecoverFromLog(m, masterLog) // Replay log to restore state after a crash
	
	// Constantly check worker heartbeats
	go m.checkWorkers()
	log.Printf("Starting %d workers, %d map tasks, %d reduce tasks", nWorkers, len(files), nReduce)

	// Launch n workers
	var wg sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			NewWorker(id, m).run()	// Asks master for tasks and executes them
		}()
	}

	wg.Wait()	// Block until all gorotunites done
	
	merge()	// Merge output partitions into single file
	log.Printf("MapReduce complete. Word counts are in %s.", finalOut)
}

func merge() {
	// Read all output partition files
	partitions, _ := filepath.Glob("mr-out-*")

	// Process each line in partition
	var lines []string
	for _, p := range partitions {
		f, err := os.Open(p)
		if err != nil {
			log.Printf("merge: cannot open %s: %v", p, err)
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			if t := scanner.Text(); t != "" {
				lines = append(lines, t)
			}
		}
		f.Close()
	}

	sort.Strings(lines)	// Sort all lines alphabetically

	// Write sorted lines to single output file
	out, err := os.Create(finalOut)
	if err != nil {
		log.Printf("merge: cannot create %s: %v", finalOut, err)
		return
	}
	for _, l := range lines {
		fmt.Fprintln(out, l)
	}
	out.Close()

	for _, p := range partitions {
		// Remove intermediate partition files
		os.Remove(p)	
	}
}

func cleanup() {
	// Remove left over log files
	os.Remove(masterLog)
	os.Remove(finalOut)
	
	matches, _ := filepath.Glob("mr-out-*")
	for _, f := range matches {
		os.Remove(f)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `Fault-tolerant MapReduce — word frequency counter

Usage:
  go run . [-backup a.log,b.log] <nworkers> <nreduce> <input-files...>

  -backup     comma-separated backup log filenames placed in logs/ (optional)
  nworkers    number of worker goroutines to spawn
  nreduce     number of reduce partitions
  input-files one or more text files to count words in

Fault tolerance:
  Worker failure  — each worker sends heartbeats every 2s; if the master
                    sees no heartbeat for 6s it resets that worker's tasks
                    so another worker can pick them up.
  Master failure  — the master writes a write-ahead log to logs/master.log
                    and any backup logs before every state change; restarting
                    with the same arguments replays the log and resumes.

Examples:
  go run . 3 3 ../pg-being_ernest.txt ../pg-metamorphosis.txt
  go run . -backup backup.log 4 2 ../pg-being_ernest.txt ../pg-metamorphosis.txt

Output: output/mr-out-final (merged and sorted)
Logs:   logs/master.log (and logs/<backup> if -backup is set)
`)
}

func mustAtoi(s string) int {
// Parse string as an int and log if it fails
	var n int
	if _, err := fmt.Sscan(s, &n); err != nil {
		log.Fatalf("expected integer, got %q: %v", s, err)
	}
	return n
}
