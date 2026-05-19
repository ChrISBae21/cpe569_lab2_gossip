# Lab 4 — Fault-Tolerant MapReduce (Word Frequency)
## How to Run

All commands are run from the `src/` directory.

```
cd lab4/src
go run . <nworkers> <nreduce> <input-files...>
```

| `nworkers` | Number of worker goroutines |
| `nreduce` | Number of reduce partitions (more = more parallelism in reduce phase) |
| `input-files` | One or more `.txt` files to count words in |

### Basic example

```
go run . 3 3 ../pg-being_ernest.txt ../pg-metamorphosis.txt
```

### With backup log replication

```
go run . -backup backup.log 3 3 ../pg-being_ernest.txt ../pg-metamorphosis.txt
```

The `-backup` flag takes a comma-separated list of filenames. Backup logs are written to `lab4/logs/`.

## Output

| `lab4/output/mr-out-final` | Merged, alphabetically sorted word counts |
| `lab4/logs/master.log` | Write-ahead log for master crash recovery |

Each run automatically deletes output and logs from the previous run before starting.


