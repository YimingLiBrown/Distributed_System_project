/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package tapestry

// __BEGIN_TA__
import (
	"fmt"
	"time"
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)

// __END_TA__
/* __BEGIN_STUDENT__
import (
	"fmt"
	// Uncomment for xtrace
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)
__END_STUDENT__ */

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
	replicas, err := local.Lookup(key)
	if err != nil {
		return nil, err
	}
	if len(replicas) == 0 {
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
	// __BEGIN_TA__
	// xtr.NewTask("publish")
	publish := func(key string) error {
		Debug.Printf("Publishing %v\n", key)

		failures := 0
		for failures < RETRIES {
			// Route to the root node
			root, err := local.findRoot(local.node, Hash(key))
			if err != nil {
				failures++
				continue
			}

			// Register our local node on the root
			isRoot, err := root.RegisterRPC(key, local.node)
			if err != nil {
				// xtr.AddTags("failure")
				// Trace.Printf("failed to publish, bad node: %v\n", root)
				local.RemoveBadNodes([]RemoteNode{root})
				failures++
			} else if !isRoot {
				Trace.Printf("failed to publish to %v, not the root node", root)
				failures++
			} else {
				Trace.Printf("Successfully published %v on %v", key, root)
				return nil
			}
		}

		// xtr.AddTags("failure")
		// Trace.Printf("failed to publish %v (%v) due to %v/%v failures", key, Hash(key), failures, RETRIES)
		return fmt.Errorf("Unable to publish %v (%v) due to %v/%v failures", key, Hash(key), failures, RETRIES)
	}

	// Publish the key immediately
	err = publish(key)
	if err != nil {
		return
	}

	// Create the done channel
	done = make(chan bool)

	// Periodically republish the key
	go func() {
		ticker := time.NewTicker(REPUBLISH)

		for {
			select {
			case <-ticker.C:
				{
					// xtr.NewTask("republish")
					Trace.Printf("republishing %v", key)
					err := publish(key)
					if err != nil {
						Error.Print(err)
					}
				}
			case <-done:
				{
					Trace.Printf("Stopping advertisement of %v", key)
					//fmt.Printf("Stopping advertisement for %v\n", key)
					return
				}
			}
		}
	}()
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// Look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the replicas (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
func (local *Node) Lookup(key string) (nodes []RemoteNode, err error) {
	// __BEGIN_TA__
	// xtr.NewTask("lookup")
	Trace.Printf("Looking up %v", key)

	// Function to look up a key
	lookup := func(key string) ([]RemoteNode, error) {
		// Look up the root node
		node, err := local.findRoot(local.node, Hash(key))
		if err != nil {
			return nil, err
		}

		// Get the replicas from the root's location map
		isRoot, nodes, err := node.FetchRPC(key)
		if err != nil {
			return nil, err
		} else if !isRoot {
			return nil, fmt.Errorf("Root node did not believe it was the root node")
		} else {
			return nodes, nil
		}
	}

	// Attempt up to RETRIES many times
	errs := make([]error, 0, RETRIES)
	for len(errs) < RETRIES {
		Debug.Printf("Looking up %v, attempt=%v\n", key, len(errs)+1)
		results, lookuperr := lookup(key)
		if lookuperr != nil {
			Error.Println(lookuperr)
			errs = append(errs, lookuperr)
			continue
		}

		return results, nil
	}

	err = fmt.Errorf("%v failures looking up %v: %v", RETRIES, key, errs)

	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// Returns the best candidate from our routing table for routing to the provided ID
func (local *Node) GetNextHop(id ID, level int32) (nexthop RemoteNode, err error, toRemove *NodeSet) {
	// __BEGIN_TA__
	nexthop, err, toRemove = local.table.GetNextHop(id, level)
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// Register the specified node as an advertiser of the specified key.
// - Check that we are the root node for the key
// - Add the node to the location map
// - Kick off a timer to remove the node if it's not advertised again after a set amount of time
func (local *Node) Register(key string, replica RemoteNode) (isRoot bool, err error) {
	// __BEGIN_TA__
	node, _, _ := local.table.GetNextHop(Hash(key), 0)
	if node == local.node {
		isRoot = true
		if local.locationsByKey.Register(key, replica, TIMEOUT) {
			Debug.Printf("Register %v:%v (%v)\n", key, replica, Hash(key))
		}
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// - Check that we are the root node for the requested key
// - Return all nodes that are registered in the local location map for this key
func (local *Node) Fetch(key string) (isRoot bool, replicas []RemoteNode, err error) {
	// __BEGIN_TA__
	node, _, _ := local.table.GetNextHop(Hash(key), 0)
	if node == local.node {
		isRoot = true
		replicas = local.locationsByKey.Get(key)
		Debug.Printf("Lookup %v:%v (%v)\n", key, replicas, Hash(key))
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return
}

// - Register all of the provided objects in the local location map
// - If appropriate, add the from node to our local routing table
func (local *Node) Transfer(from RemoteNode, replicamap map[string][]RemoteNode) (err error) {
	// __BEGIN_TA__
	if len(replicamap) > 0 {
		Debug.Printf("Registering objects from %v: %v\n", from, replicamap)
		local.locationsByKey.RegisterAll(replicamap, TIMEOUT)
	}
	local.addRoute(from)
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__
	return nil
}

// Utility function for iteratively contacting nodes to get the root node for the provided ID.
//
// -  Starting from the specified node, iteratively contact nodes calling getNextHop until we reach the root node
// -  Also keep track of any bad nodes that errored during lookup
// -  At each step, notify the next-hop node of all of the bad nodes we have encountered along the way
func (local *Node) findRoot(start RemoteNode, id ID) (RemoteNode, error) {
	//__BEGIN_TA__

	// Keep track of faulty nodes along the way
	root, success, err := local.GetCache(id)
	sharedPrefix := SharedPrefixLength(root.Id, id)
	rootcheck, _, _ := root.GetNextHopRPC(id, int32(sharedPrefix))
	if !success || root.Id != rootcheck.Id {
		root, err, _ = start.GetNextHopRPC(id, 0)
		local.StoreCache(id, root)
		if err != nil {
			return RemoteNode{}, fmt.Errorf("Err")
		}
	}
	root, err, _ = start.GetNextHopRPC(id, 0)
	if err != nil {
		return RemoteNode{}, fmt.Errorf("Unable to get root for %v, all nodes traversed were bad, starting from %v", id, start)
	}

	// local.cache[id] = Cache{
	// 	node: root,
	// 	timer: ,
	// }
	return root, err
}

func (local *Node) GetCache(id ID) (RemoteNode, bool, error) {
	cachemap := local.cache
	prev, ok := cachemap[id]
	if ok {
		prevTime := prev.timer
		if int64(time.Now().Nanosecond())-int64(prevTime) < CACHE_TIMEOUT.Nanoseconds() {
			return prev.node, true, nil
		} else {
			delete(local.cache, id)
		}
	}
	return RemoteNode{}, false, nil

}

func (local *Node) StoreCache(id ID, root RemoteNode) error {
	cache := Cache{
		node:  root,
		timer: uint64(time.Now().Nanosecond()),
	}
	local.cache[id] = cache
	return nil
}

// var counter int32= 0
// // Use a stack to keep track of the traversed nodes
// nodes := make([]RemoteNode, 0)
// nodes = append(nodes, start)
// tail := 0

// // The remote node will indicate if it thinks its the root
// for len(nodes) > 0 {
// 	current := nodes[tail]
// 	nodes = nodes[:tail]
// 	tail--

// 	// If necessary, notify the next hop of accumulated bad nodes
// 	if badnodes.Size() > 0 {
// 		err := current.RemoveBadNodesRPC(badnodes.Nodes())
// 		if err != nil {
// 			// TODO: route error message somewhere?
// 			badnodes.Add(current)
// 			continue
// 		}
// 	}

// 	// Get the next hop
// 	//morehops, next, err := current.GetNextHopRPC(id, 0, badnodes)
// 	counter += 1
// 	if err != nil {
// 		// TODO: route error message somewhere?
// 		badnodes.Add(current)
// 		continue
// 	}

// 	// Check if we've arrived at the key's root
// 	if !morehops {
// 		return current, nil
// 	}

// 	// Traverse the next node
// 	nodes = append(nodes, current, next)
// 	tail += 2
//}

//return RemoteNode{}, fmt.Errorf("Unable to get root for %v, all nodes traversed were bad, starting from %v", id, start)
// __END_TA__
/* __BEGIN_STUDENT__
    // TODO: students should implement this
    return local.node, nil
__END_STUDENT__ */
