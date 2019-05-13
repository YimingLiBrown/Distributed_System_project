package raft

import (
	"bytes"
	"fmt"
	"google.golang.org/grpc/grpclog"
	"io/ioutil"
	"os/user"
	"runtime"
	"time"
)

const (
	WAIT_PERIOD = 6
)

func suppressLoggers() {
	Out.SetOutput(ioutil.Discard)
	Error.SetOutput(ioutil.Discard)
	Debug.SetOutput(ioutil.Discard)
	grpclog.SetLogger(Out)
}

// Creates a cluster of nodes at specific ports, with a
// more lenient election timeout for testing.
func createTestCluster(ports []int) ([]*RaftNode, error) {
	runtime.GOMAXPROCS(8)
	SetDebug(false)
	config := DefaultConfig()
	config.ClusterSize = len(ports)
	config.ElectionTimeout = time.Millisecond * 400
	// Use a path in /tmp/ so we use local disk and not NFS
	curUser, _ := user.Current()
	config.LogPath = "/tmp/" + curUser.Username + "_raftlogs"
	if runtime.GOOS == "windows" {
		config.LogPath = "raftlogs/"
	}
	return CreateDefinedLocalCluster(config, ports)
}

// Returns the leader in a raft cluster, and an error otherwise.
func findLeader(nodes []*RaftNode) (*RaftNode, error) {
	leaders := make([]*RaftNode, 0)
	for _, node := range nodes {
		if node.State == LEADER_STATE {
			leaders = append(leaders, node)
		}
	}

	if len(leaders) == 0 {
		return nil, fmt.Errorf("No leader found in slice of nodes")
	} else if len(leaders) == 1 {
		return leaders[0], nil
	} else {
		return nil, fmt.Errorf("Found too many leaders in slice of nodes: %v", len(leaders))
	}
}

// Returns whether all logs in a cluster match the leader's.
func logsMatch(leader *RaftNode, nodes []*RaftNode) bool {
	for _, node := range nodes {
		if node.State != LEADER_STATE {
			if bytes.Compare(node.stateMachine.GetState().([]byte), leader.stateMachine.GetState().([]byte)) != 0 {
				return false
			}
		}
	}
	return true
}

// Given a slice of RaftNodes representing a cluster,
// exits each node and removes its logs.
func cleanupCluster(nodes []*RaftNode) {
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		node.server.Stop()
		go func(node *RaftNode) {
			node.GracefulExit()
			node.RemoveLogs()
		}(node)
	}
	time.Sleep(5 * time.Second)
}
