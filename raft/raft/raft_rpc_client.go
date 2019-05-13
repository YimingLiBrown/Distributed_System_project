// Brown University, CS138, Spring 2019
//
// Purpose: Provides wrappers around the client interface of GRPC to invoke
// functions on remote Raft nodes.

package raft

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var dialOptions []grpc.DialOption

func init() {
	dialOptions = []grpc.DialOption{grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(2 * time.Second)}
}

// RPC Invocation functions

var connMap = make(map[string]*grpc.ClientConn)
var connMapLock = &sync.RWMutex{}

func closeAllConnections() {
	connMapLock.Lock()
	defer connMapLock.Unlock()
	for k, conn := range connMap {
		conn.Close()
		delete(connMap, k)
	}
}

// makeClientConn creates a new client connection to the given remote node
func makeClientConn(remote *RemoteNode) (*grpc.ClientConn, error) {
	return grpc.Dial(remote.Addr, dialOptions...)
}

// ClientConn creates or returns a cached RPC client for the given remote node
func (remote *RemoteNode) ClientConn() (RaftRPCClient, error) {
	connMapLock.RLock()
	if cc, ok := connMap[remote.Addr]; ok {
		connMapLock.RUnlock()
		return NewRaftRPCClient(cc), nil
	}
	connMapLock.RUnlock()

	cc, err := makeClientConn(remote)
	if err != nil {
		return nil, err
	}
	connMapLock.Lock()
	connMap[remote.Addr] = cc
	connMapLock.Unlock()

	return NewRaftRPCClient(cc), err
}

// RemoveClientConn removes the client connection to the given node, if present
func (remote *RemoteNode) RemoveClientConn() {
	connMapLock.Lock()
	defer connMapLock.Unlock()
	if cc, ok := connMap[remote.Addr]; ok {
		cc.Close()
		delete(connMap, remote.Addr)
	}
}

// connCheck checks the given error and removes the client connection if it's not nil
func (remote *RemoteNode) connCheck(err error) error {
	if err != nil {
		remote.RemoveClientConn()
	}
	return err
}

// JoinRPC tells the given remote node that we (a new Raft node) want to join the cluster
func (remote *RemoteNode) JoinRPC(local *RaftNode) error {
	// Get a client connection for the remote node
	if local.NetworkPolicy.IsDenied(*local.GetRemoteSelf(), *remote) {
		return ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		fmt.Printf("DEBUG: err: %v\n", err)
		return err
	}

	// Call the actual RPC call over the network to the remote node
	ok, err := cc.JoinCaller(context.Background(), local.GetRemoteSelf())
	if err == nil && !ok.GetOk() {
		return fmt.Errorf("unable to join Raft cluster")
	}

	// Check for an RPC error, and close the client connection if necessary.
	// Be sure to use this in the rest of the functions in this file!
	return remote.connCheck(err)
}

// StartNodeRPC tells the remote node to start execution with the given list
// of RemoteNodes as the list of all the nodes in the cluster.
func (remote *RemoteNode) StartNodeRPC(local *RaftNode, nodeList []RemoteNode) error {
	// TODO: Students should implement this method
	if local.NetworkPolicy.IsDenied(*local.GetRemoteSelf(), *remote) {
		return ErrorNetworkPolicyDenied
	}
	cc, err1 := remote.ClientConn()
	if err1 != nil {
		return err1
	}
	//var nodelist []*RemoteNode
	pnodeList := make([]*RemoteNode, len(nodeList))
	for i := 0; i < len(nodeList); i++ {
		// fmt.Printf("i: %v", i)
		pnodeList[i] = &nodeList[i]
	}
	_, err2 := cc.StartNodeCaller(context.Background(), &StartNodeRequest{
		FromNode: remote,
		NodeList: pnodeList,
	})

	return remote.connCheck(err2)
	// Make sure to check the provided raftNode's network policy to make sure
	// we are allowed to make this request.
}

// AppendEntriesRPC is called by a leader in the cluster attempting to append
// entries to one of its followers. Be sure to pass in a pointer to the RaftNode
// making the request.
func (remote *RemoteNode) AppendEntriesRPC(local *RaftNode, request *AppendEntriesRequest) (*AppendEntriesReply, error) {
	// TODO: Students should implement this method？？？？？？？？？
	// Make sure to check the provided raftNode's network policy to make sure
	// we are allowed to make this request.
	if local.NetworkPolicy.IsDenied(*local.GetRemoteSelf(), *remote) {
		return nil, ErrorNetworkPolicyDenied
	}
	// if local.State!=LEADER_STATE{//？？？？？？？
	// 	return AppendEntriesReply{},nil
	// }
	cc, err1 := remote.ClientConn()
	if err1 != nil {
		return nil, err1
	}

	reply, err2 := cc.AppendEntriesCaller(context.Background(), request)
	//Out.Print("APPENDENTRIESRPC return\n")
	return reply, err2
}

// RequestVoteRPC asks the given remote node for a vote, using the provided
// RequestVoteRequest struct as the request. Note that calling nodes should
// pass in a pointer to their own RaftNode struct.
func (remote *RemoteNode) RequestVoteRPC(local *RaftNode, request *RequestVoteRequest) (*RequestVoteReply, error) {
	// TODO: Students should implement this method
	// Make sure to check the provided raftNode's network policy to make sure
	// we are allowed to make this request.
	if local.NetworkPolicy.IsDenied(*local.GetRemoteSelf(), *remote) {
		return nil, ErrorNetworkPolicyDenied
	}
	cc, err1 := remote.ClientConn()
	if err1 != nil {
		return nil, err1
	}
	reply, err2 := cc.RequestVoteCaller(context.Background(), request)
	return reply, err2
}

// RegisterClientRPC is called by a new client trying to register itself with
// the given Raft node in the cluster.
func (remote *RemoteNode) RegisterClientRPC() (*RegisterClientReply, error) {
	// TODO: Students should implement this method
	cc, err1 := remote.ClientConn()
	if err1 != nil {
		return nil, err1
	}
	reply, err := cc.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	return reply, err
}

// ClientRequestRPC is executed by a client trying to make a request to the
// given Raft node in the cluster.
func (remote *RemoteNode) ClientRequestRPC(request *ClientRequest) (*ClientReply, error) {
	// TODO: Students should implement this method

	cc, err1 := remote.ClientConn()
	if err1 != nil {
		return nil, err1
	}
	reply, err := cc.ClientRequestCaller(context.Background(), request)
	if err != nil {
		return nil, err
	}
	return reply, err
}
