package main

import (
	"fmt"
	"math/rand"
	"time"
)

const follower = 0
const candidate = 1
const leader = 2

const leaderTimeout = 100 * time.Millisecond
const maxElectionTimeout = 300
const minElectionTimeout = 150
const votesTimeout = 150 * time.Millisecond

func randElectionTimeout() time.Duration {
	return time.Duration(minElectionTimeout+rand.Intn(maxElectionTimeout-minElectionTimeout)) * time.Millisecond
}

func StartRaft(node *Node) {
	leaderTicker := time.NewTicker(leaderTimeout)
	electionTicker := time.NewTicker(randElectionTimeout())
	voteTimer := time.NewTimer(votesTimeout)
	voteTimer.Stop() // Run once an election begins

	voteCount := 0
	votesReceived := map[int]bool{}

	for {
		select {
		case <-node.quit:
			leaderTicker.Stop()
			electionTicker.Stop()
			voteTimer.Stop()
			return

		case msg := <-node.raft_info.channel:
			switch msg.msgType {
			case msgHeartbeat:
				// Process heartbeat from leader
				if msg.term >= node.raft_info.term {
					if msg.term > node.raft_info.term {
						node.raft_info.term = msg.term
						node.raft_info.votedFor = -1
					}

					electionTicker.Reset(randElectionTimeout()) // Reset timeout since leader still alive
					node.raft_info.state = follower
				}

			case msgRequestVote:
				// Process requests to vote for candidates
				if msg.term > node.raft_info.term {
					node.raft_info.term = msg.term
					node.raft_info.votedFor = -1
					node.raft_info.state = follower
				}

				if msg.term == node.raft_info.term && (node.raft_info.votedFor == -1 || node.raft_info.votedFor == msg.id) {
					// Only respond if newer term and havent voted already in term or already voting for this node
					node.raft_info.votedFor = msg.id
					electionTicker.Reset(randElectionTimeout())

					response_msg := RaftMessage{
						msgType:  msgVoteResponse,
						id:       node.id,
						state:    node.raft_info.state,
						term:     node.raft_info.term,
						votedFor: node.raft_info.votedFor,
					}

					select {
					case node.neighbors[msg.id].raft_info.channel <- response_msg:
						// Sent vote response
					default:
						// Dropped message usually because sending to a dead node
					}
				}

			case msgVoteResponse:
				// Process vote responses
				if node.raft_info.state == candidate && msg.term == node.raft_info.term {
					if msg.votedFor == node.id && !votesReceived[msg.id] {

						printMu.Lock()
						fmt.Printf("Node %d received a vote\n", node.id)
						printMu.Unlock()

						votesReceived[msg.id] = true // Avoid processing duplicate vote responses
						voteCount++                  // Received a vote
					}
				}

			}

		case <-leaderTicker.C:
			if node.raft_info.state == leader {
				SendLeaderHeartbeat(node)

				printMu.Lock()
				fmt.Printf("Leader Node: %d sending heartbeat\n", node.id)
				printMu.Unlock()
			}

		case <-electionTicker.C:
			if node.raft_info.state != follower {
				continue
			}

			printMu.Lock()
			fmt.Printf("Node %d starting election\n", node.id)
			printMu.Unlock()

			// Leader is dead -> begin election
			node.raft_info.state = candidate
			node.raft_info.term++ // Increment to next term

			node.raft_info.votedFor = node.id
			voteCount = 1 // Vote for yourself
			votesReceived = map[int]bool{node.id: true}

			// Send vote requests
			candidate_msg := RaftMessage{
				msgType: msgRequestVote,
				id:      node.id,
				state:   node.raft_info.state,
				term:    node.raft_info.term,
			}

			printMu.Lock()
			fmt.Printf("Node %d sending vote requests\n", node.id)
			printMu.Unlock()

			voteTimer.Reset(votesTimeout)

			for i := range nodeCount {
				if i != node.id {
					select {
					case node.neighbors[i].raft_info.channel <- candidate_msg:
						// Sent vote response
					default:
						// Dropped message usually because sending to a dead node
					}
				}
			}

		case <-voteTimer.C:
			if node.raft_info.state == candidate {

				printMu.Lock()
				fmt.Printf("Node %d counting votes\n", node.id)
				printMu.Unlock()

				res := ProcessVotes(node, voteCount)
				if !res {
					electionTicker.Reset(randElectionTimeout()) // Reset timeout
				} else {
					SendLeaderHeartbeat(node) // Send heartbeat after winning
				}
			}
		}
	}
}

func SendLeaderHeartbeat(node *Node) {
	// Send leader heartbeat to all neighbors
	leader_msg := RaftMessage{
		msgType: msgHeartbeat,
		id:      node.id,
		state:   node.raft_info.state,
		term:    node.raft_info.term,
	}

	for i := range nodeCount {
		if i != node.id {
			select {
			case node.neighbors[i].raft_info.channel <- leader_msg:
				// Sent vote response
			default:
				// Dropped message usually because sending to a dead node
			}
		}
	}
}

func ProcessVotes(node *Node, count int) bool {
	if node.raft_info.state == candidate {
		if count > nodeCount/2 {
			node.raft_info.state = leader // Won election

			printMu.Lock()
			fmt.Printf("Node %d WON\n", node.id)
			printMu.Unlock()

			return true
		} else {
			node.raft_info.state = follower // Lost election

			printMu.Lock()
			fmt.Printf("Node %d LOST\n", node.id)
			printMu.Unlock()

			return false
		}
	}
	return false
}
