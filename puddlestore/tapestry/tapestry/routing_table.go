/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
	"sync"
)

// A routing table has a number of levels equal to the number of digits in an ID
// (default 40). Each level has a number of slots equal to the digit base
// (default 16). A node that exists on level n thereby shares a prefix of length
// n with the local node. Access to the routing table protected by a mutex.
type RoutingTable struct {
	local RemoteNode                  // The local tapestry node
	rows  [DIGITS][BASE]*[]RemoteNode // The rows of the routing table
	mutex sync.Mutex                  // To manage concurrent access to the routing table (could also have a per-level mutex)
}

// Creates and returns a new routing table, placing the local node at the
// appropriate slot in each level of the table.
func NewRoutingTable(me RemoteNode) *RoutingTable {
	t := new(RoutingTable)
	t.local = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			slot := make([]RemoteNode, 0, SLOTSIZE)
			t.rows[i][j] = &slot
		}
	}

	// Make sure each row has at least our node in it
	for i := 0; i < DIGITS; i++ {
		slot := t.rows[i][t.local.Id[i]]
		*slot = append(*slot, t.local)
	}

	return t
}

// Adds the given node to the routing table.
//
// Returns true if the node did not previously exist in the table and was subsequently added.
// Returns the previous node in the table, if one was overwritten.
func (t *RoutingTable) Add(node RemoteNode) (added bool, previous *RemoteNode) {
	// __BEGIN_TA__
	// Check we aren't re-adding ourselves
	if t.local.Id == node.Id {
		return
	}

	// Get the level of the table where this node should go
	level := t.level(node)
	digit := node.Id[level]
	// __END_TA__

	t.mutex.Lock()

	// __BEGIN_TA__
	slot := t.rows[level][digit]
	if len(*slot) == SLOTSIZE {
		added, previous = doReplace(t.local, node, slot)
	} else {
		added = doAdd(node, slot)
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__

	t.mutex.Unlock()

	return
}

// Removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
func (t *RoutingTable) Remove(node RemoteNode) (wasRemoved bool) {
	// __BEGIN_TA__
	// Cannot remove ourselves from the table
	if t.local == node {
		return false
	}

	// Determine the level and slot the node belongs in
	level := t.level(node)
	digit := node.Id[level]
	// __END_TA__

	t.mutex.Lock()

	// __BEGIN_TA__
	wasRemoved = doRemove(node, t.rows[level][digit])
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__

	t.mutex.Unlock()

	return
}

// Get all nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodes []RemoteNode) {
	// __BEGIN_TA__
	if level < 0 || level >= DIGITS {
		return nil
	}

	nodes = make([]RemoteNode, 0, BASE*SLOTSIZE)
	// __END_TA__

	t.mutex.Lock()

	// __BEGIN_TA__
	for _, slot := range t.rows[level] {
		for _, node := range *slot {
			if node != t.local {
				nodes = append(nodes, node)
			}
		}
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__

	t.mutex.Unlock()

	return
}

// Search the table for the closest next-hop node for the provided ID.
func (t *RoutingTable) GetNextHop(id ID, level int32) (node RemoteNode, err error, toRemove *NodeSet) {
	toRemove = NewNodeSet()
	t.mutex.Lock()
	for{
		if level == DIGITS - 1 {
			t.mutex.Unlock()
			return t.local, nil, toRemove

		}
		node = t.doGetNodeAtLevel(level, id)
		if node == t.local {
			level += 1
			continue 
		}
		t.mutex.Unlock()
		root, err, addToRemove := node.GetNextHopRPC(id, level + 1)
		if err != nil{
			toRemove.Add(node)
			t.Remove(node)
			t.mutex.Lock()
			continue
		}
		toRemove.AddAll(addToRemove.Nodes())
		for _, rem := range(toRemove.Nodes()){
			t.Remove(rem)
		}
		return root, nil, toRemove
	}
}
/*
	// __BEGIN_TA__
	for i := level; i < DIGITS; i++ {
		node = t.doGetNodeAtLevel(i, id)

		// break when we find a better candidate than our node
		if node != t.local {
			node = node.table.GetNextHop(id, level+1)
		}
	}
	// __END_TA__
	// __BEGIN_STUDENT__
	// TODO: students should implement this
	// __END_STUDENT__

	t.mutex.Unlock()

	return
}
*/
// __BEGIN_TA__

// Private non-locking implementation.
func (t *RoutingTable) doGetNodeAtLevel(d int32, id ID) (node RemoteNode) {
	// Get the d'th row, then cycle through slots until we find a node
	row := t.rows[d]
	digit := id[d]
	for i := 0; i < BASE; i++ {
		slot := row[digit]
		if len(*slot) > 0 {
			return closest(id, *slot)
		}
		digit = (digit + 1) % BASE
	}

	return t.local
}

func (t *RoutingTable) level(node RemoteNode) int {
	return SharedPrefixLength(t.local.Id, node.Id)
}

// Removes all occurrences of toRemove from nodes.
func doRemove(toRemove RemoteNode, nodes *[]RemoteNode) (wasRemoved bool) {
	size := len(*nodes)
	for i := 0; i < size; i++ {
		if (*nodes)[i] == toRemove {
			lastnode := (*nodes)[size-1]
			(*nodes)[size-1] = toRemove
			(*nodes)[i] = lastnode
			*nodes = (*nodes)[:size-1]
			i--
			wasRemoved = true
			size--
		}
	}
	return
}

// If the new node is closer than an existing node, the existing node is replaced.
func doReplace(local RemoteNode, newNode RemoteNode, existingNodes *[]RemoteNode) (existingNodeWasReplaced bool, previous *RemoteNode) {
	// First, check the node isn't already in the list
	for i := 0; i < len(*existingNodes); i++ {
		if (*existingNodes)[i] == newNode {
			return false, nil
		}
	}

	// Now, try replacing an existing node with the new node
	furthest := newNode
	for i := 0; i < len(*existingNodes); i++ {
		existing := (*existingNodes)[i]
		if local.Id.Closer(furthest.Id, existing.Id) {
			(*existingNodes)[i] = furthest
			furthest = existing
			existingNodeWasReplaced = true
		}
	}
	if furthest != newNode {
		previous = &furthest
	}
	return
}

// Add a node to the list so long as it's not already present
func doAdd(newNode RemoteNode, existingNodes *[]RemoteNode) (wasAdded bool) {
	for i := 0; i < len(*existingNodes); i++ {
		if (*existingNodes)[i] == newNode {
			return false
		}
	}
	*existingNodes = append(*existingNodes, newNode)
	return true
}

// Returns the closest node in the list to the provided ID
func closest(id ID, nodes []RemoteNode) (closest RemoteNode) {
	closest = nodes[0]
	for _, node := range nodes {
		if id.Closer(node.Id, closest.Id) {
			closest = node
		}
	}
	return
}

// __END_TA__
