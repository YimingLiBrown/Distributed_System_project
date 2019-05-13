package raft

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *RaftNode) doCandidate() stateFunction {
	r.Out("Transitioning to CANDIDATE_STATE")
	r.State = CANDIDATE_STATE
	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	resultChan := make(chan bool)
	fallbackChan := make(chan bool)
	// vote for self
	cnt := int(1)
	r.setVotedFor(r.GetRemoteSelf().GetId())
	// increase current term
	r.setCurrentTerm(r.GetCurrentTerm() + 1)
	RandomTimeOut := randomTimeout(r.config.ElectionTimeout) //reset timeout
	r.requestVotes(resultChan, fallbackChan, r.GetCurrentTerm())

	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		case voteMsg := <-r.requestVote:
			fallback := r.handleCompetingRequestVote(voteMsg)
			if fallback {
				return r.doFollower
			}
		case entriesMsg := <-r.appendEntries:
			_, fallback := r.handleAppendEntries(entriesMsg)
			if fallback {
				return r.doFollower
			}
		case <-RandomTimeOut:
			return r.doCandidate
		case FB := <-fallbackChan:
			if FB {
				return r.doFollower
			}
		case result := <-resultChan:
			if result {
				cnt++
			}
			if cnt >= r.config.ClusterSize/2+1 {
				return r.doLeader
			}

		case clientMsg := <-r.registerClient:
			clientMsg.reply <- RegisterClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				LeaderHint: r.Leader,
			}

		case clientRequest := <-r.clientRequest:
			clientRequest.reply <- ClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				LeaderHint: r.Leader,
			}
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (r *RaftNode) requestVotes(electionResults chan bool, fallback chan bool, currTerm uint64) {
	// TODO: Students should implement this method
	req := RequestVoteRequest{
		Term:         r.GetCurrentTerm(),
		Candidate:    r.GetRemoteSelf(),
		LastLogIndex: r.getLastLogIndex(),
		LastLogTerm:  r.getLogEntry(r.getLastLogIndex()).GetTermId(),
	}
	for _, node := range r.GetNodeList() {
		if r.GetRemoteSelf().GetId() != node.GetId() {
			go func(request RequestVoteRequest, node RemoteNode) {
				reply, err1 := node.RequestVoteRPC(r, &request)
				if err1 != nil {
					Out.Printf("something wrong with RequestVoteRPC:%v\n", err1)
				} else {
					if reply.GetTerm() > r.GetCurrentTerm() {
						fallback <- true
						electionResults <- reply.GetVoteGranted()
					} else {
						fallback <- false
						electionResults <- reply.GetVoteGranted()
					}
				}
			}(req, node)
		}
	}
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
//------------------------------------------------------------------------------------//
func (r *RaftNode) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {

	// TODO: Students should implement this method
	if msg.request.GetTerm() > r.GetCurrentTerm() {
		msg.reply <- RequestVoteReply{
			Term:        msg.request.GetTerm(),
			VoteGranted: true,
		}
		r.setVotedFor(msg.request.Candidate.GetId())
		r.setCurrentTerm(msg.request.GetTerm())
		return true
	} else if msg.request.GetTerm() == r.GetCurrentTerm() {
		if msg.request.GetLastLogIndex() > r.getLastLogIndex() {
			msg.reply <- RequestVoteReply{
				Term:        msg.request.GetTerm(),
				VoteGranted: true,
			}
			r.setVotedFor(msg.request.Candidate.GetId())
			return true
		}
	}
	msg.reply <- RequestVoteReply{
		Term:        r.GetCurrentTerm(),
		VoteGranted: false,
	}
	return false
}
