package raft

import (
	"sort"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (r *RaftNode) doLeader() stateFunction {
	r.Out("Transitioning to LEADER_STATE")
	r.State = LEADER_STATE

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.

	for _, node := range r.GetNodeList() {
		r.leaderMutex.Lock()
		r.matchIndex[node.GetAddr()] = 0
		r.nextIndex[node.GetAddr()] = r.getLastLogIndex() + 1
		r.leaderMutex.Unlock()
	}
	r.leaderMutex.Lock()
	firstentry := LogEntry{
		Index:  r.getLastLogIndex() + 1,
		TermId: r.GetCurrentTerm(),
		Type:   CommandType_NOOP,
	}
	r.appendLogEntry(firstentry)
	r.leaderMutex.Unlock()
	fallback, _ := r.sendHeartbeats()
	//for leader, don't need to use the randomTimeout
	timeout := time.After(r.config.HeartbeatTimeout)

	if fallback {
		return r.doFollower
	}

	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		case <-timeout:
			fallback, _ := r.sendHeartbeats()
			timeout = time.After(r.config.HeartbeatTimeout)
			if fallback {
				return r.doFollower
			}
			// ALWAYS TRY TO COMMIT
			matchIndexes := make([]int, 0)
			r.leaderMutex.Lock()
			for _, matchIndex := range r.matchIndex {
				matchIndexes = append(matchIndexes, int(matchIndex))
			}
			sort.Ints(matchIndexes)
			newCommitIndex := matchIndexes[len(matchIndexes)/2]
			for i := r.commitIndex + 1; i <= uint64(newCommitIndex); i++ {
				r.processLogEntry(*r.getLogEntry(i))
			}
			r.commitIndex = uint64(newCommitIndex)
			r.leaderMutex.Unlock()
		case clientRequest := <-r.clientRequest:
			// CHECK CACHE;
			// CREATE A CACHE ID
			// MAKE AN ENTRY AND APPEND
			r.requestsMutex.Lock()
			request := clientRequest.request
			cacheID := createCacheId(request.GetClientId(), request.GetSequenceNum())
			r.requestsByCacheId[cacheID] = clientRequest.reply
			r.requestsMutex.Unlock()

			if _, ok := r.stableState.ClientReplyCache[cacheID]; ok {
				clientRequest.reply <- r.stableState.ClientReplyCache[cacheID]
			} else {
				entry := LogEntry{
					Index:   r.getLastLogIndex() + 1,
					TermId:  r.GetCurrentTerm(),
					Type:    CommandType_STATE_MACHINE_COMMAND,
					Command: request.GetStateMachineCmd(),
					Data:    request.GetData(),
					CacheId: cacheID,
				}
				r.leaderMutex.Lock()
				r.appendLogEntry(entry)
				r.leaderMutex.Unlock()
			}
		case registerClient := <-r.registerClient:
			entry := LogEntry{
				Index:  r.getLastLogIndex() + 1,
				TermId: r.GetCurrentTerm(),
				Type:   CommandType_CLIENT_REGISTRATION,
				Data:   make([]byte, 0),
			}
			r.leaderMutex.Lock()
			r.appendLogEntry(entry)
			r.leaderMutex.Unlock()
			fallback, sentToM := r.sendHeartbeats()
			timeout = time.After(r.config.HeartbeatTimeout)
			if fallback {
				reply := RegisterClientReply{
					Status:     ClientStatus_REQ_FAILED,
					ClientId:   0,
					LeaderHint: r.GetRemoteSelf(),
				}
				registerClient.reply <- reply
				return r.doFollower
			}
			if sentToM {
				matchIndexes := make([]int, 0)
				r.leaderMutex.Lock()
				for _, matchIndex := range r.matchIndex {
					matchIndexes = append(matchIndexes, int(matchIndex))
				}
				sort.Ints(matchIndexes)
				newCommitIndex := matchIndexes[len(matchIndexes)/2]

				for i := r.commitIndex + 1; i <= uint64(newCommitIndex); i++ {
					r.processLogEntry(*r.getLogEntry(i))
				}
				r.commitIndex = uint64(newCommitIndex)
				r.leaderMutex.Unlock()
				reply := RegisterClientReply{
					Status:     ClientStatus_OK,
					ClientId:   r.getLastLogIndex(),
					LeaderHint: r.GetRemoteSelf(),
				}
				registerClient.reply <- reply
			} else {
				reply := RegisterClientReply{
					Status:     ClientStatus_REQ_FAILED,
					ClientId:   0,
					LeaderHint: r.GetRemoteSelf(),
				}
				registerClient.reply <- reply
			}

		case voteRequest := <-r.requestVote:
			fallback := r.handleCompetingRequestVote(voteRequest)
			if fallback {
				return r.doFollower
			}

		case appendRequest := <-r.appendEntries:
			_, fallback := r.handleAppendEntries(appendRequest)
			if fallback {
				return r.doFollower
			}
		}
	}
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (r *RaftNode) sendHeartbeats() (fallback, sentToMajority bool) {
	// TODO: Students should implement this method
	Out.Printf("%v sending hb\n", r.GetRemoteSelf().GetId())
	fallbackchan := make(chan bool, r.config.ClusterSize)
	count := 0
	for _, node := range r.GetNodeList() {
		entries := make([]*LogEntry, 0)
		r.leaderMutex.Lock()
		if r.getLastLogIndex() >= r.nextIndex[node.GetAddr()] {
			for i := r.nextIndex[node.GetAddr()]; i <= r.getLastLogIndex(); i++ {
				entries = append(entries, r.getLogEntry(i))
			}
		}
		appendRequest := AppendEntriesRequest{
			Term:         r.GetCurrentTerm(),
			Leader:       r.GetRemoteSelf(),
			PrevLogIndex: r.nextIndex[node.GetAddr()] - 1,
			PrevLogTerm:  r.getLogEntry(r.nextIndex[node.GetAddr()] - 1).GetTermId(),
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		r.leaderMutex.Unlock()

		if node.GetId() != r.GetRemoteSelf().GetId() {
			go func(req AppendEntriesRequest, node RemoteNode) {
				reply, err := node.AppendEntriesRPC(r, &req)
				if err != nil {
					fallbackchan <- false
					return
				} else {
					if reply.GetTerm() > r.GetCurrentTerm() {
						r.setCurrentTerm(reply.GetTerm())
						r.setVotedFor("")
						fallbackchan <- true
					} else {
						if reply.GetSuccess() != true {
							// nextIndex--
							r.leaderMutex.Lock()
							r.nextIndex[node.GetAddr()]--
							r.leaderMutex.Unlock()
							fallbackchan <- false
						} else {
							// update nextIndex and matchIndex
							r.leaderMutex.Lock()
							r.nextIndex[node.GetAddr()] = r.getLastLogIndex() + 1
							r.matchIndex[node.GetAddr()] = r.getLastLogIndex()
							count++
							r.leaderMutex.Unlock()
							fallbackchan <- false
						}
					}
				}
			}(appendRequest, node)
		}
	}

	// traverse fallbackchan
	for i := 0; i < len(r.GetNodeList())-1; i++ {
		select {
		case fb := <-fallbackchan:
			if fb {
				return true, true
			}
		}
	}
	//Out.Printf("count = %v, half = %v\n", count, r.config.ClusterSize/2+1)
	if count >= r.config.ClusterSize/2+1 {
		return false, true
	}
	return false, false
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (r *RaftNode) processLogEntry(entry LogEntry) ClientReply {
	Out.Printf("Processing log entry: %v\n", entry)

	status := ClientStatus_OK
	response := ""
	var err error

	// Apply command on state machine
	if entry.Type == CommandType_STATE_MACHINE_COMMAND {
		response, err = r.stateMachine.ApplyCommand(entry.Command, entry.Data)
		if err != nil {
			status = ClientStatus_REQ_FAILED
			response = err.Error()
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		Response:   response,
		LeaderHint: r.GetRemoteSelf(),
	}

	// Add reply to cache
	if entry.CacheId != "" {
		r.CacheClientReply(entry.CacheId, reply)
	}

	// Send reply to client
	r.requestsMutex.Lock()
	replyChan, exists := r.requestsByCacheId[entry.CacheId]
	if exists {
		replyChan <- reply
		delete(r.requestsByCacheId, entry.CacheId)
	}
	r.requestsMutex.Unlock()

	return reply
}
