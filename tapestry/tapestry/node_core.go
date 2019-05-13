/*
 *  Brown University, CS138, Spring 2019
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package tapestry

import (
	"fmt"
	"time"
)

// Store a blob on the local node and publish the key to the tapestry.
func (local *Node) Store(key string, value []byte) (err error) {
	done, err := local.Publish(key)
	if err != nil {
		return err
	}
	local.blobstore.Put(key, value, done)
	return nil
}

// Lookup a key in the tapestry then fetch the corresponding blob from the
// remote blob store.
func (local *Node) Get(key string) ([]byte, error) {
	// Lookup the key

	replicas, err := local.Lookup(key) //something goes wrong with the lookup
	if err != nil {
		return nil, err
	}
	if len(replicas) == 0 { //no such replicas
		return nil, fmt.Errorf("No replicas returned for key %v", key)
	}

	// Contact replicas
	var errs []error
	for _, replica := range replicas {
		blob, err := replica.BlobStoreFetchRPC(key)
		if err != nil {
			errs = append(errs, err)
		}
		if blob != nil {
			return *blob, nil
		}
	}

	return nil, fmt.Errorf("Error contacting replicas, %v: %v", replicas, errs)
}

// Remove the blob from the local blob store and stop advertising
func (local *Node) Remove(key string) bool {
	return local.blobstore.Delete(key)
}

// Publishes the key in tapestry.
//
// - Route to the root node for the key
// - Register our ln node on the root
// - Start periodically republishing the key
// - Return a channel for cancelling the publish
func (local *Node) Publish(key string) (done chan bool, err error) {
	// TODO: students should implement this
	done = make(chan bool)
	//root, err := local.findRoot(local.node, MakeID(key))
	id := Hash(key)
	level := SharedPrefixLength(local.node.Id, id)
	root, err, _ := local.GetNextHop(id, int32(level))

	_, err = root.RegisterRPC(key, local.node)
	timer := time.Tick(REPUBLISH)

	select {
	case <-timer:
		go local.Publish(key)
	case <-done:
		return

	}
	return done, err

}

// Look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the replicas (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
func (local *Node) Lookup(key string) (nodes []RemoteNode, err error) {
	// TODO: students should implement this
	id := Hash(key)
	root, err := local.findRoot(local.node, id)
	if err != nil {
		return nodes, fmt.Errorf("Error when finding root: %v", err)
	}
	done, replicas, err := root.FetchRPC(key)
	if err != nil {
		fmt.Printf("Error when fetching the replicas: %v", err)
		i := 0
		for !done && i < RETRIES-1 {
			done, replicas, err = root.FetchRPC(key)
			i++
		}
	}

	return replicas, err
}

// Returns the best candidate from our routing table for routing to the provided ID.
// That node should recursively get the best candidate from its routing table, so the result of calling this should be
// the best candidate in the network for the given id.
func (local *Node) GetNextHop(id ID, level int32) (nexthop RemoteNode, err error, toRemove *NodeSet) {
	// TODO: students should implement this
	nexthop, err, toRemove = local.table.GetNextHop(id, level)
	return nexthop, err, toRemove
}

// Register the specified node as an advertiser of the specified key.
// - Check that we are the root node for the key
// - Add the node to the location map
// - Kick off a timer to remove the node if it's not advertised again after a set amount of time
func (local *Node) Register(key string, replica RemoteNode) (isRoot bool, err error) {
	// TODO: students should implement this
	id := Hash(key)
	root, err := local.findRoot(local.node, id)
	if err != nil {
		return isRoot, fmt.Errorf("something goes wrong when finding root: %v", err)
	}
	if root == local.node {
		isRoot = true
		bool := local.locationsByKey.Register(key, replica, TIMEOUT) //????when local node is the root???
		if !bool {
			local.Remove(key)
		}
	}

	return isRoot, err
}

// - Check that we are the root node for the requested key
// - Return all nodes that are registered in the local location map for this key
func (local *Node) Fetch(key string) (isRoot bool, replicas []RemoteNode, err error) {
	// TODO: students should implement this
	id := Hash(key)
	root, err := local.findRoot(local.node, id)
	if err != nil {
		return isRoot, replicas, fmt.Errorf("something goes wrong when finding root: %v", err)
	}
	if root == local.node {
		isRoot = true
	}
	replicas = local.locationsByKey.Get(key)
	return isRoot, replicas, err
}

// - Register all of the provided objects in the local location map
// - If appropriate, add the from node to our local routing table
func (local *Node) Transfer(from RemoteNode, replicamap map[string][]RemoteNode) (err error) {
	// TODO: students should implement this
	local.locationsByKey.RegisterAll(replicamap, TIMEOUT)
	err = local.addRoute(from)
	if err != nil {
		return fmt.Errorf("Error in registering object: %v", err)
	}
	return err
}

// Utility function for iteratively contacting nodes to get the root node for the provided ID.
//
// -  Starting from the specified node, call your recursive GetNextHop. This should
//    continually call GetNextHop until the root node is returned.
// -  Also keep track of any bad nodes that errored during lookup, and
//    as you come back from your recursive calls, remove the bad nodes from local
func (local *Node) findRoot(start RemoteNode, id ID) (RemoteNode, error) {
	// TODO: students should implement this
	level := SharedPrefixLength(start.Id, id)

	root, err, toRemove := start.GetNextHopRPC(id, int32(level))
	if err != nil {
		return root, fmt.Errorf("Error finding bad nodes")
	} //what should be the input of the second parameter?
	if toRemove != nil {
		err = local.RemoveBadNodes(toRemove.Nodes()) //move all the badnodes
	}
	if err != nil {
		return root, fmt.Errorf("Error removing bad nodes")
	}
	return root, err
}
