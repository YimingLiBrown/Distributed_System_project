package raft

import (
	"fmt"
	"testing"
	"time"
)

// Tests that nodes can successfully join a cluster and elect a leader.
func TestInit(t *testing.T) {
	// Out.Println("TestInit start")
	suppressLoggers()
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WAIT_PERIOD)

	followers, candidates, leaders := 0, 0, 0
	for i := 0; i < config.ClusterSize; i++ {
		node := cluster[i]
		switch node.State {
		case FOLLOWER_STATE:
			followers++
		case CANDIDATE_STATE:
			candidates++
		case LEADER_STATE:
			leaders++
		}
	}

	if followers != config.ClusterSize-1 {
		t.Errorf("follower count mismatch, expected %v, got %v", config.ClusterSize-1, followers)
	}

	if candidates != 0 {
		t.Errorf("candidate count mismatch, expected %v, got %v", 0, candidates)
	}

	if leaders != 1 {
		t.Errorf("leader count mismatch, expected %v, got %v", 1, leaders)
	}
	// Out.Println("TestInit done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}

// Tests that if a leader is partitioned from its followers, a
// new leader is elected.
func TestNewElection(t *testing.T) {
	// Out.Println("TestNewElection start")
	suppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 5

	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WAIT_PERIOD)
	oldLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("~~~~~~~~~~~~OLD LEADER: %v, state: %v\n", oldLeader.GetRemoteSelf().GetId(), oldLeader.State)
	// partition leader, triggering election
	oldTerm := oldLeader.GetCurrentTerm()
	oldLeader.NetworkPolicy.PauseWorld(true)

	// wait for new leader to be elected
	time.Sleep(time.Second * WAIT_PERIOD)

	// unpause old leader and wait for it to become a follower
	oldLeader.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * WAIT_PERIOD)
	//Out.Printf("OLD LEADER is: %v\n", oldLeader.State.String())
	newLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("~~~~~~~~~~~~NEW LEADER: %v, state: %v\n", newLeader.GetRemoteSelf().GetId(), newLeader.State)
	fmt.Printf("~~~~~~~~~~~~old old LEADER: %v, state: %v\n", oldLeader.GetRemoteSelf().GetId(), oldLeader.State)
	if oldLeader.Id == newLeader.Id {
		t.Errorf("leader did not change")
	}

	if newLeader.GetCurrentTerm() == oldTerm {
		t.Errorf("term did not change")
	}
	// Out.Println("TestNewElection done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}
