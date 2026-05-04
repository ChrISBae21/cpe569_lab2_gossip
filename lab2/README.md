# lab2_gossip

For running the GoRoutines version of the lab, go into the GoRoutines folder and simply run the command

```bash
go run .
```

For running the RPC version, to run a server node run the command

```bash
go run .\server.go
```

but for the 8 client nodes run the client.go file and input the node ID. For example here is how you would run a node with an ID of 1

```bash
go run .\client.go 1
```