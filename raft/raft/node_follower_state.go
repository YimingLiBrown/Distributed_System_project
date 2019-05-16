package raft

// doFollower implements the logic for a Raft node in the follower state.
func (r *RaftNode) doFollower() stateFunction {
	r.Out("Transitioning to FOLLOWER_STATE")
	r.State = FOLLOWER_STATE

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.
	RandomTimeOut := randomTimeout(r.config.ElectionTimeout)
	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}

		case entriesMsg := <-r.appendEntries:
			reset, _ := r.handleAppendEntries(entriesMsg)
			if reset {
				RandomTimeOut = randomTimeout(r.config.ElectionTimeout)
			}

		case voteMsg := <-r.requestVote:
			request := voteMsg.request
			if request.GetTerm() < r.GetCurrentTerm() {
				voteMsg.reply <- RequestVoteReply{
					Term:        r.GetCurrentTerm(),
					VoteGranted: false,
				}
			} else if r.GetVotedFor() == "" || r.GetVotedFor() == request.GetCandidate().GetId() {
				if request.GetLastLogTerm() > r.getLogEntry(r.getLastLogIndex()).GetTermId() {
					voteMsg.reply <- RequestVoteReply{
						Term:        request.GetTerm(),
						VoteGranted: true,
					}
					r.setVotedFor(request.GetCandidate().GetId())
				} else if request.GetLastLogTerm() == r.getLogEntry(r.getLastLogIndex()).GetTermId() && request.GetLastLogIndex() >= r.getLastLogIndex() {
					voteMsg.reply <- RequestVoteReply{
						Term:        request.GetTerm(),
						VoteGranted: true,
					}
					r.setVotedFor(request.GetCandidate().GetId())
				}
			} else {
				voteMsg.reply <- RequestVoteReply{
					Term:        r.GetCurrentTerm(),
					VoteGranted: false,
				}
			}

		case <-RandomTimeOut:
			return r.doCandidate

		case clientMsg := <-r.registerClient:
			clientMsg.reply <- RegisterClientReply{
				Status:     ClientStatus_NOT_LEADER,
				LeaderHint: r.Leader,
			}

		case clientReplyMsg := <-r.clientRequest:
			clientReplyMsg.reply <- ClientReply{
				Status:     ClientStatus_NOT_LEADER,
				LeaderHint: r.Leader,
			}

		}
	}
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (r *RaftNode) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	// TODO: Students should implement this method

	prevIndex := msg.request.GetPrevLogIndex()
	Entries := msg.request.Entries

	if msg.request.GetTerm() < r.GetCurrentTerm() {
		reply := AppendEntriesReply{
			Term:    r.GetCurrentTerm(),
			Success: false,
		}
		msg.reply <- reply
		return false, false
	} else {
		//update node term to leader's term
		r.setCurrentTerm(msg.request.GetTerm())
		r.setVotedFor("")
		if r.getLastLogIndex() != 0 && msg.request.GetPrevLogTerm() != r.getLogEntry(prevIndex).GetTermId() {
			reply := AppendEntriesReply{
				Term:    r.GetCurrentTerm(),
				Success: false,
			}
			msg.reply <- reply
			return true, true
		} else {
			r.leaderMutex.Lock()
			if msg.request.GetPrevLogIndex()+1 != r.getLastLogIndex() {
				r.truncateLog(msg.request.GetPrevLogIndex() + 1)
			}
			for _, entry := range Entries {
				r.appendLogEntry(*entry)
			}
			r.leaderMutex.Unlock()

			// Take the smaller one
			r.leaderMutex.Lock()

			if msg.request.LeaderCommit > r.commitIndex {
				if msg.request.LeaderCommit < r.getLastLogIndex() {
					r.commitIndex = msg.request.LeaderCommit
				} else {
					r.commitIndex = r.getLastLogIndex()
				}
			}
			// Process new entries
			if r.commitIndex > r.lastApplied {
				for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
					r.processLogEntry(*r.getLogEntry(i))
					r.lastApplied++
				}
			}
			r.leaderMutex.Unlock()
			reply := AppendEntriesReply{
				Term:    msg.request.GetTerm(),
				Success: true,
			}
			msg.reply <- reply
		}
	}
	return true, true
}
