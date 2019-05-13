/*
 *  Brown University, CS138, Spring 2019
 *
 *  Purpose: Defines global constants and functions to create and join a new
 *  node into a Tapestry mesh, and functions for altering the routing table
 *  and backpointers of the local node that are invoked over RPC.
 */

package tapestry

import (
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	// Uncomment for xtrace
	// util "github.com/brown-csci1380/tracing-framework-go/xtrace/grpcutil"
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)

const BASE = 16    // The base of a digit of an ID.  By default, a digit is base-16.
const DIGITS = 40  // The number of digits in an ID.  By default, an ID has 40 digits.
const RETRIES = 3  // The number of retries on failure. By default we have 3 retries.
const K = 10       // During neighbor traversal, trim the neighborset to this size before fetching backpointers. By default this has a value of 10.
const SLOTSIZE = 3 // The each slot in the routing table should store this many nodes. By default this is 3.

const REPUBLISH = 10 * time.Second // Object republish interval for nodes advertising objects.
const TIMEOUT = 25 * time.Second   // Object timeout interval for nodes storing objects.

// The main struct for the local Tapestry node. Methods can be invoked locally on this struct.
type Node struct {
	node           RemoteNode    // The ID and address of this node
	table          *RoutingTable // The routing table
	backpointers   *Backpointers // Backpointers to keep track of other nodes that point to us
	locationsByKey *LocationMap  // Stores keys for which this node is the root
	blobstore      *BlobStore    // Stores blobs on the local node
	server         *grpc.Server
}

func (local *Node) String() string {
	return fmt.Sprintf("Tapestry Node %v at %v", local.node.Id, local.node.Address)
}

// Called in tapestry initialization to create a tapestry node struct
func newTapestryNode(node RemoteNode) *Node {
	serverOptions := []grpc.ServerOption{}
	// Uncomment for xtrace
	// serverOptions = append(serverOptions, grpc.UnaryInterceptor(util.XTraceServerInterceptor))
	n := new(Node)

	n.node = node
	n.table = NewRoutingTable(node)
	n.backpointers = NewBackpointers(node)
	n.locationsByKey = NewLocationMap()
	n.blobstore = NewBlobStore()
	n.server = grpc.NewServer(serverOptions...)

	return n
}

// Start a tapestry node on the specified port. Optionally, specify the address
// of an existing node in the tapestry mesh to connect to; otherwise set to "".
func Start(port int, connectTo string) (*Node, error) {
	return start(RandomID(), port, connectTo)
}

// Private method, useful for testing: start a node with the specified ID rather than a random ID.
func start(id ID, port int, connectTo string) (tapestry *Node, err error) {

	// Create the RPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	// Get the hostname of this machine
	name, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Unable to get hostname of local machine to start Tapestry node. Reason: %v", err)
	}

	// Get the port we are bound to
	_, actualport, err := net.SplitHostPort(lis.Addr().String()) //fmt.Sprintf("%v:%v", name, port)
	if err != nil {
		return nil, err
	}

	// The actual address of this node
	address := fmt.Sprintf("%s:%s", name, actualport)

	// Uncomment for xtrace
	// xtr.NewTask("startup")
	// Trace.Print("Tapestry Starting up...")
	// xtr.SetProcessName(fmt.Sprintf("Tapestry %X... (%v)", id[:5], address))

	// Create the local node
	tapestry = newTapestryNode(RemoteNode{Id: id, Address: address})
	fmt.Printf("Created tapestry node %v\n", tapestry)
	Trace.Printf("Created tapestry node")

	RegisterTapestryRPCServer(tapestry.server, tapestry)
	fmt.Printf("Registered RPC Server\n")
	go tapestry.server.Serve(lis)

	// If specified, connect to the provided address
	if connectTo != "" {
		// Get the node we're joining
		node, err := SayHelloRPC(connectTo, tapestry.node)
		if err != nil {
			return nil, fmt.Errorf("Error joining existing tapestry node %v, reason: %v", address, err)
		}
		err = tapestry.Join(node)
		if err != nil {
			return nil, err
		}
	}
	return tapestry, nil
}

// Invoked when starting the local node, if we are connecting to an existing Tapestry.
//
// - Find the root for our node's ID
// - Call AddNode on our root to initiate the multicast and receive our initial neighbor set
// - Iteratively get backpointers from the neighbor set and populate routing table
func (local *Node) Join(otherNode RemoteNode) (err error) {
	Debug.Println("Joining", otherNode)
	// Route to our root
	root, err := local.findRoot(otherNode, local.node.Id)
	if err != nil {
		//fmt.Println("DEBUG: errors finding root?")
		return fmt.Errorf("Error joining existing tapestry node %v, reason: %v", otherNode, err)
	}
	// Add ourselves to our root by invoking AddNode on the remote node
	neighbors, err := root.AddNodeRPC(local.node)

	if err != nil {
		return fmt.Errorf("Error adding ourselves to root node %v, reason: %v", root, err)
	}

	// Add the neighbors to our local routing table.

	for _, n := range neighbors {
		//fmt.Printf("JOIN: addRoute: n: %v", n)
		local.addRoute(n)
	}
	// TODO: students should implement the backpointer traversal portion of Join
	level := SharedPrefixLength(local.node.Id, otherNode.Id)

	local.TraverseBackpointers(neighbors, level)

	return nil
}

// HELPER FUNCTION:
func (local *Node) TraverseBackpointers(neighbors []RemoteNode, level int) {
	if level >= 0 {
		nextNeighbors := neighbors
		for _, neighbor := range neighbors {
			temp, err := neighbor.GetBackpointersRPC(neighbor, level)
			if err != nil {
				fmt.Printf("ERROR Traversal: %v\n", err)
			}
			nextNeighbors = append(nextNeighbors, temp...)
		}

		for _, nNbrs := range nextNeighbors {
			local.table.Add(nNbrs)
		}
		length := len(nextNeighbors)
		quickSort(nextNeighbors, 0, length-1)

		keys := make(map[RemoteNode]bool)
		var results []RemoteNode
		for _, node := range nextNeighbors {
			if _, value := keys[node]; !value {
				keys[node] = true
				results = append(results, node)
			}
		}
		nextNeighbors = results

		if len(nextNeighbors) > K {
			nextNeighbors = nextNeighbors[0:K]
		}

		local.TraverseBackpointers(nextNeighbors, level-1)
	}
	return
}

func quickSort(values []RemoteNode, left int, right int) {
	temp := values[left]
	p := left
	i, j := left, right
	for i <= j {
		for j >= p && temp.Id.big().Cmp(values[j].Id.big()) != 1 {
			j--
		}
		if j >= p {
			values[p] = values[j]
			p = j
		}

		for i <= p && temp.Id.big().Cmp(values[i].Id.big()) != -1 {
			i++
		}
		if i <= p {
			values[p] = values[i]
			p = i
		}
	}
	values[p] = temp
	if p-left > 1 {
		quickSort(values, left, p-1)
	}
	if right-p > 1 {
		quickSort(values, p+1, right)
	}
}

// We are the root node for some new node joining the tapestry.
//
// - Begin the acknowledged multicast
// - Return the neighborset from the multicast
func (local *Node) AddNode(node RemoteNode) (neighborset []RemoteNode, err error) {
	return local.AddNodeMulticast(node, SharedPrefixLength(node.Id, local.node.Id))
}

// A new node is joining the tapestry, and we are a need-to-know node participating in the multicast.
//
// - Propagate the multicast to the specified row in our routing table
// - Await multicast response and return the neighborset
// - Add the route for the new node
// - Begin transfer of appropriate replica info to the new node
func (local *Node) AddNodeMulticast(newnode RemoteNode, level int) (neighbors []RemoteNode, err error) {
	if level < DIGITS {
		targets := local.table.GetLevel(level)
		targets = append(targets, local.node)
		results := make([]RemoteNode, 0)
		for _, target := range targets {
			tempRes, err := target.AddNodeMulticastRPC(newnode, level+1)
			if err != nil {
				// DEAL WITH BAD NODES: target
				fmt.Printf("ERROR in multicast: %v\n", err)
				local.RemoveBadNodes([]RemoteNode{target})
				return nil, err
			}
			results = append(results, tempRes...)
		}
		results = append(results, targets...)

		local.addRoute(newnode)

		newnode.TransferRPC(local.node, local.locationsByKey.GetTransferRegistrations(local.node, newnode))

		neighbors = make([]RemoteNode, 0)
		temp := map[RemoteNode]struct{}{}
		for _, item := range results {
			if _, ok := temp[item]; !ok {
				temp[item] = struct{}{}
				neighbors = append(neighbors, item)
			}
		}
	}
	return neighbors, err
}

// Add the from node to our backpointers, and possibly add the node to our
// routing table, if appropriate
func (local *Node) AddBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Add(from) {
		Debug.Printf("Added backpointer %v\n", from)
	}
	local.addRoute(from)
	return
}

// Remove the from node from our backpointers
func (local *Node) RemoveBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Remove(from) {
		Debug.Printf("Removed backpointer %v\n", from)
	}
	return
}

// Get all backpointers at the level specified, and possibly add the node to our
// routing table, if appropriate
func (local *Node) GetBackpointers(from RemoteNode, level int) (backpointers []RemoteNode, err error) {
	Debug.Printf("Sending level %v backpointers to %v\n", level, from)
	backpointers = local.backpointers.Get(level)
	local.addRoute(from)
	return
}

// The provided nodes are bad and we should discard them
// - Remove each node from our routing table
// - Remove each node from our set of backpointers
func (local *Node) RemoveBadNodes(badnodes []RemoteNode) (err error) {
	for _, badnode := range badnodes {
		if local.table.Remove(badnode) {
			fmt.Printf("Removed bad node %v\n", badnode)
		}
		if local.backpointers.Remove(badnode) {
			fmt.Printf("Removed bad node backpointer %v\n", badnode)
		}
	}
	return
}

// Utility function that adds a node to our routing table.
//
// - Adds the provided node to the routing table, if appropriate.
// - If the node was added to the routing table, notify the node of a backpointer
// - If an old node was removed from the routing table, notify the old node of a removed backpointer
func (local *Node) addRoute(node RemoteNode) (err error) {
	//fmt.Println("DEBUG: addRoute")
	added, previous := local.table.Add(node)
	if added {
		err = node.AddBackpointerRPC(local.node)
		if err != nil {
			fmt.Printf("addRoute err: %v", err)
			return err
		}
	}
	if previous != nil {
		err = previous.RemoveBackpointerRPC(local.node)
		if err != nil {
			fmt.Printf("addRoute err2: %v", err)
			return err
		}
	}
	return err
}
