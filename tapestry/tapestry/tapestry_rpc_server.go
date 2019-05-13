/*
 *  Brown University, CS138, Spring 2019
 *
 *  Purpose: Implements functions that are invoked by other nodes over RPC.
 */

package tapestry

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"
)

/**
 * RPC receiver functions
 */

func (local *Node) HelloCaller(ctx context.Context, n *NodeMsg) (*NodeMsg, error) {
	return local.node.toNodeMsg(), nil
}

func (local *Node) GetNextHopCaller(ctx context.Context, id *IdMsg) (*NextHop, error) {
	idVal, err := ParseID(id.Id)
	if err != nil {
		return nil, err
	}
	next, err, tr := local.GetNextHop(idVal, id.Level)
	rsp := &NextHop{
		Next:     next.toNodeMsg(),
		ToRemove: remoteNodesToNodeMsgs(tr.Nodes()),
	}
	return rsp, err
}

func (local *Node) RegisterCaller(ctx context.Context, r *Registration) (*Ok, error) {
	// TODO: students should implement this
	var nodeMsgs []*NodeMsg
	replicaMsg := r.GetFromNode()
	nodeMsgs = append(nodeMsgs, replicaMsg)
	replicaSet := nodeMsgsToRemoteNodes(nodeMsgs)
	_, err := local.Register(r.GetKey(), replicaSet[0])
	rsp := &Ok{
		Ok: true,
	}
	return rsp, err
}

func (local *Node) FetchCaller(ctx context.Context, key *Key) (*FetchedLocations, error) {
	isRoot, values, err := local.Fetch(key.Key)

	rsp := &FetchedLocations{
		Values: remoteNodesToNodeMsgs(values),
		IsRoot: isRoot,
	}
	return rsp, err
}

func (local *Node) RemoveBadNodesCaller(ctx context.Context, nodes *Neighbors) (*Ok, error) {
	// TODO: students should implement this
	badnodes := nodeMsgsToRemoteNodes(nodes.GetNeighbors())
	err := local.RemoveBadNodes(badnodes)
	rsp := &Ok{
		Ok: true,
	}
	return rsp, err
}

func (local *Node) AddNodeCaller(ctx context.Context, n *NodeMsg) (*Neighbors, error) {
	neighbors, err := local.AddNode(n.toRemoteNode())
	if err != nil {
		fmt.Printf("ERROR in caller: %v\n", err)
	}
	rsp := &Neighbors{
		Neighbors: remoteNodesToNodeMsgs(neighbors),
	}
	return rsp, err
}

func (local *Node) AddNodeMulticastCaller(ctx context.Context, m *MulticastRequest) (*Neighbors, error) {
	nb, err := local.AddNodeMulticast(m.GetNewNode().toRemoteNode(), int(m.GetLevel()))
	return &Neighbors{
		Neighbors: remoteNodesToNodeMsgs(nb),
	}, err
}

func (local *Node) TransferCaller(ctx context.Context, td *TransferData) (*Ok, error) {
	parsedData := make(map[string][]RemoteNode)
	for key, set := range td.Data {
		parsedData[key] = nodeMsgsToRemoteNodes(set.Neighbors)
	}
	err := local.Transfer(td.From.toRemoteNode(), parsedData)

	rsp := &Ok{
		Ok: true,
	}
	return rsp, err
}

func (local *Node) AddBackpointerCaller(ctx context.Context, n *NodeMsg) (*Ok, error) {
	// TODO: students should implement this
	err := local.AddBackpointer(n.toRemoteNode())
	rsp := &Ok{
		Ok: true,
	}
	return rsp, err
}

func (local *Node) RemoveBackpointerCaller(ctx context.Context, n *NodeMsg) (*Ok, error) {
	err := local.RemoveBackpointer(n.toRemoteNode())
	rsp := &Ok{
		Ok: true,
	}
	return rsp, err
}

func (local *Node) GetBackpointersCaller(ctx context.Context, br *BackpointerRequest) (*Neighbors, error) {
	// TODO: students should implement this
	var nodeMsgs []*NodeMsg
	fromNodemsg := br.GetFrom()
	nodeMsgs = append(nodeMsgs, fromNodemsg)
	fromSet := nodeMsgsToRemoteNodes(nodeMsgs)
	neighbors, err := local.GetBackpointers(fromSet[0], int(br.GetLevel()))
	rsp := &Neighbors{
		Neighbors: remoteNodesToNodeMsgs(neighbors),
	}
	return rsp, err
}

func (local *Node) NotifyLeaveCaller(ctx context.Context, ln *LeaveNotification) (*Ok, error) {
	replacement := ln.Replacement.toRemoteNode()
	err := local.NotifyLeave(ln.From.toRemoteNode(), &replacement)
	rsp := &Ok{
		Ok: true,
	}
	return rsp, err
}

func (local *Node) BlobStoreFetchCaller(ctx context.Context, key *Key) (*DataBlob, error) {
	data, isOk := local.blobstore.Get(key.Key)
	var err error
	if !isOk {
		err = errors.New("Key not found")
	}
	return &DataBlob{
		Key:  key.Key,
		Data: data,
	}, err
}

func (local *Node) TapestryLookupCaller(ctx context.Context, key *Key) (*Neighbors, error) {
	nodes, err := local.Lookup(key.Key)
	return &Neighbors{remoteNodesToNodeMsgs(nodes)}, err
}

func (local *Node) TapestryStoreCaller(ctx context.Context, blob *DataBlob) (*Ok, error) {
	return &Ok{Ok: true}, local.Store(blob.Key, blob.Data)
}

func remoteNodesToNodeMsgs(remoteNodes []RemoteNode) []*NodeMsg {
	nodeMsgs := make([]*NodeMsg, len(remoteNodes))
	for i, thing := range remoteNodes {
		nodeMsgs[i] = thing.toNodeMsg()
	}
	return nodeMsgs
}

func nodeMsgsToRemoteNodes(nodeMsgs []*NodeMsg) []RemoteNode {
	remoteNodes := make([]RemoteNode, len(nodeMsgs))
	for i, thing := range nodeMsgs {
		remoteNodes[i] = thing.toRemoteNode()
	}
	return remoteNodes
}
