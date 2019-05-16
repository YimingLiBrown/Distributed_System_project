package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/raft/hashmachine"
	"golang.org/x/net/context"
)

// Example test making sure leaders can register the client and process the request from clients properly
func TestClientInteraction_Leader(t *testing.T) {
	// Out.Println("TestClientInteraction_Leader start")
	suppressLoggers()
	config := DefaultConfig()
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("~~~~~~~~~~~~~~LEADER: %v\n", leader)
	// First make sure we can register a client correctly
	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	if reply.Status != ClientStatus_OK {
		t.Fatal("Counld not register client")
	}

	clientid := reply.ClientId

	// Hash initialization request
	initReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HASH_CHAIN_INIT,
		Data:            []byte("hello"),
	}
	clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Make sure further request is correct processed
	ClientReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     2,
		StateMachineCmd: hashmachine.HASH_CHAIN_ADD,
		Data:            []byte{},
	}
	clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	leader.GetRemoteSelf().RemoveClientConn()
	closeAllConnections()
	// Out.Println("TestClientInteraction_Leader done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}

// Example test making sure the follower would reject the registration and requests from clients with correct messages
// The test on candidates can be similar with these sample tests
func TestClientInteraction_Follower(t *testing.T) {
	// Out.Println("TestClientInteraction_Follower start")
	suppressLoggers()
	config := DefaultConfig()
	// set the ElectionTimeout long enough to keep nodes in the state of follower
	config.ElectionTimeout = 60 * time.Second
	config.ClusterSize = 3
	config.HeartbeatTimeout = 500 * time.Millisecond
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	// make sure the client get the correct response while registering itself with a follower
	reply, _ := cluster[0].RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	fmt.Printf("reply request: %v\n", reply.Status)
	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to a follower")
	}

	// make sure the client get the correct response while sending a request to a follower
	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HASH_CHAIN_INIT,
		Data:            []byte("hello"),
	}
	clientResult, _ := cluster[0].ClientRequestCaller(context.Background(), &req)
	fmt.Printf("client request1: %v\n", clientResult.Status)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a follower")
	}
	// Out.Println("TestClientInteraction_Follower done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}

// newly added test
//the client registers itself to a caldidate, and the raft should return a hint to the
//client that this node is not a leader.
func TestClientInteraction_Candidate(t *testing.T) {
	// Out.Println("TestClientInteraction_Candidate start")
	suppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 7
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(3 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("~~~~~~~~~~~~~~LEADER: %v\n", leader)

	for _, node := range cluster {
		if node.State == CANDIDATE_STATE {
			fmt.Println("find the candidate!")
			reply, _ := node.GetRemoteSelf().RegisterClientRPC()
			fmt.Printf("reply request: %v\n", reply.Status)
			if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
				t.Error(reply.Status)
				t.Fatal("Wrong response when registering a client to a candidate")
			}

			req := ClientRequest{
				ClientId:        1,
				SequenceNum:     1,
				StateMachineCmd: hashmachine.HASH_CHAIN_INIT,
				Data:            []byte("hello"),
			}
			clientResult, _ := node.GetRemoteSelf().ClientRequestRPC(&req)
			fmt.Printf("client request1: %v\n", clientResult.Status)
			if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
				t.Fatal("Wrong response when sending a client request to a candidate")
			}
			break
		}
	}
	// Out.Println("TestClientInteraction_Candidate done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}

// newly added test
//the client registers itself to the old leader, and the raft should return a hint to the
//client that this node is not a leader.
func TestClientInteraction_oldleader(t *testing.T) {
	// Out.Println("TestClientInteraction_oldleader start")
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
	fmt.Printf("~~~~~~~~~~~~OLD LEADER: %v\n", oldLeader.GetRemoteSelf().GetId())
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
	fmt.Printf("~~~~~~~~~~~~NEW LEADER: %v\n", newLeader.GetRemoteSelf().GetId())

	if oldLeader.Id == newLeader.Id {
		t.Errorf("leader did not change")
	}

	if newLeader.GetCurrentTerm() == oldTerm {
		t.Errorf("term did not change")
	}

	//register the client to the old leader
	reply, _ := oldLeader.GetRemoteSelf().RegisterClientRPC()
	fmt.Printf("reply request: %v\n", reply.Status)
	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to an old leader")
	}

	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HASH_CHAIN_INIT,
		Data:            []byte("hello"),
	}
	clientResult, _ := oldLeader.GetRemoteSelf().ClientRequestRPC(&req)
	fmt.Printf("client request1: %v\n", clientResult.Status)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to an old leader")
	}
	// Out.Println("TestClientInteraction_oldleader done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}
