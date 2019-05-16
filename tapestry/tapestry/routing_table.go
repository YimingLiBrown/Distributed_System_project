/*
 *  Brown University, CS138, Spring 2019
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
	"fmt"
	"sort"
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
			slot := make([]RemoteNode, 0)
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
	//fmt.Println("DEBUG: running t.Add")
	t.mutex.Lock()
	added = false

	// TODO: students should implement this
	r := SharedPrefixLength(t.local.Id, node.Id)
	if r == DIGITS {
		t.mutex.Unlock()
		return added, nil
	}
	c2 := node.Id[r] // STARTS FROM 0
	// CHECK IF THE SLOT IS FULL
	slot := *t.rows[r][c2]

	// IF IT'S ALREADY IN THE TABLE.
	for _, value := range slot {
		if value.Id.String() == node.Id.String() {
			t.mutex.Unlock()
			return added, nil
		}
	}
	if len(slot) == SLOTSIZE {
		if t.local.Id.Closer(node.Id, slot[SLOTSIZE-1].Id) {
			previous, slot = &slot[len(slot)-1], slot[:len(slot)-1]
		} else {
			t.mutex.Unlock()
			return false, previous
		}
	}

	slot = append(slot, node)
	sort.Slice(slot, func(i int, j int) bool {
		return t.local.Id.Closer(slot[i].Id, slot[j].Id)
	})

	t.rows[r][c2] = &slot

	t.mutex.Unlock()

	return true, previous

}

// Removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
func (t *RoutingTable) Remove(node RemoteNode) (wasRemoved bool) {

	t.mutex.Lock()
	wasRemoved = false
	// TODO: students should implement this
	r := SharedPrefixLength(t.local.Id, node.Id)
	c := node.Id[r]
	for i, everyNode := range *t.rows[r][c] {
		if everyNode.Id == node.Id && i < len(*t.rows[r][c])-1 {
			wasRemoved = true
			*t.rows[r][c] = append((*t.rows[r][c])[:i], (*t.rows[r][c])[i+1:]...)
		} else if everyNode.Id == node.Id && i == len(*t.rows[r][c])-1 {
			*t.rows[r][c] = (*t.rows[r][c])[:i]
		}
	}
	t.mutex.Unlock()

	return wasRemoved
}

// Get all nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodes []RemoteNode) {
	nodes = make([]RemoteNode, 0)
	if level >= DIGITS {
		return
	}
	t.mutex.Lock()
	// TODO: students should implement this
	for _, rowValue := range t.rows[level] {
		for _, slotValue := range *rowValue {
			if slotValue.Id.String() != t.local.Id.String() {
				nodes = append(nodes, slotValue)
			}
		}
	}
	t.mutex.Unlock()

	return nodes
}

// Search the table for the closest next-hop node for the provided ID.
// Recur on that node with GetNextHop, until you arrive at the root.
// keep track of bad nodes in "toRemove", and remove all known bad nodes
// locally as you unwind from your recursion.

// func (t *RoutingTable) GetNextHop(id ID, level int32) (node RemoteNode, err error, toRemove *NodeSet) {
// 	var betterChoice RemoteNode
// 	//var bestChoice RemoteNode
// 	bestChoice := t.local
// 	//var empty RemoteNode
// 	t.mutex.Lock()
// 	toRemove = NewNodeSet()
// 	// TODO: students should implement this
// 	if level < DIGITS {
// 		t.mutex.Unlock()
// 		levelNodes := t.GetLevel(int(level)) //includes 3 nodes in each slot
// 		t.mutex.Lock()
//
// 		if len(levelNodes) == 0 {
// 			levelNodes = append(levelNodes, t.local)
// 		}
//
// 		for _, remoteNode := range levelNodes {
// 			isLocal := id.BetterChoice(t.local.Id, remoteNode.Id)
// 			if isLocal {
// 				betterChoice = t.local
// 			}
//
// 			betterChoice = remoteNode
// 			if id.BetterChoice(betterChoice.Id, bestChoice.Id) {
// 				bestChoice = betterChoice
// 			}
// 		}
// 		//go to the next level
// 		//if the bestChoice is local node, call local GetNextHop
// 		if bestChoice == t.local {
// 			level++
// 			t.mutex.Unlock()
// 			node, err, toRemove = t.GetNextHop(id, level)
// 			t.mutex.Lock()
// 			t.mutex.Unlock()
// 			return bestChoice, err, toRemove
// 		} else {
// 			level++
// 			t.mutex.Unlock()
// 			node, err, toRemove = bestChoice.GetNextHopRPC(id, level)
// 			t.mutex.Lock()
// 			if err != nil {
// 				t.mutex.Unlock()
// 				toRemove.Add(bestChoice)
// 				t.mutex.Lock()
//
// 				t.mutex.Unlock()
// 				t.Remove(bestChoice)
// 				t.mutex.Lock()
//
// 				level-- //find the bestChoice again in the previous level
// 				t.mutex.Unlock()
// 				bestChoice, err, toRemove = t.GetNextHop(id, level)
// 				t.mutex.Lock()
// 				t.mutex.Unlock()
// 				return bestChoice, err, toRemove
// 			}
// 		//the bestChoice is a remoteNode in the routing table, call GetNextHopRPC
// 	}
// 	t.mutex.Unlock()
// 	return bestChoice, err, toRemove
// }
// ===================================================== //
// Debug.Printf("GetNextHop id: %v, level: %v", id, level)
// t.mutex.Lock()
//
// // TODO: students should implement this
// toRemove = NewNodeSet()
// closest := t.local
//
// if level == DIGITS {
// 	t.mutex.Unlock()
// 	return closest, err, toRemove
// }
//
// currentDigit := id[level]
// for counter := 0; counter < BASE; counter++ {
// 	slot := *t.rows[level][(int(currentDigit)+counter)%BASE]
// 	if len(slot) > 0 {
// 		closest = slot[0]
// 		for _, v := range slot {
// 			if id.Closer(v.Id, closest.Id) {
// 				closest = v
// 			}
// 		}
// 		if closest.Id.String() == t.local.Id.String() {
// 			t.mutex.Unlock()
// 			return t.local.GetNextHopRPC(id, level+1)
// 		} else {
// 			t.mutex.Unlock()
// 			node, err, rpcRemove := closest.GetNextHopRPC(id, level+1)
// 			t.mutex.Lock()
// 			if err != nil {
// 				toRemove.Add(closest)
// 				t.mutex.Unlock()
// 				t.Remove(closest)
// 				node, err, rpcRemove = t.local.GetNextHopRPC(id, level)
// 				rpcRemove.AddAll(toRemove.Nodes())
// 				return node, err, rpcRemove
// 			} else {
// 				toRemove.AddAll(rpcRemove.Nodes())
// 			}
// 			t.mutex.Unlock()
// 			return node, err, toRemove
// 		}
// 	}
// }
// return

func (t *RoutingTable) GetNextHop(id ID, level int32) (node RemoteNode, err error, toRemove *NodeSet) {
	var betterChoice RemoteNode
	bestChoice := t.local
	t.mutex.Lock()
	toRemove = NewNodeSet()
	// TODO: students should implement this
	if level < DIGITS {
		t.mutex.Unlock()
		levelNodes := t.GetLevel(int(level)) //includes 3 nodes in each slot
		t.mutex.Lock()
		//fmt.Printf("level: %v\n", level)
		if len(levelNodes) == 0 {
			levelNodes = append(levelNodes, t.local)
		}
		for _, remoteNode := range levelNodes {
			isLocal := id.BetterChoice(t.local.Id, remoteNode.Id)
			if isLocal {
				betterChoice = t.local
			} else {
				betterChoice = remoteNode
			}

			if id.BetterChoice(betterChoice.Id, bestChoice.Id) {
				bestChoice = betterChoice
			}
		}
		//go to the next level
		//if the bestChoice is local node, call local GetNextHop
		if bestChoice == t.local {
			level++
			t.mutex.Unlock()
			node, err, toRemove = t.GetNextHop(id, level)
			t.mutex.Lock()
			t.mutex.Unlock()
			return bestChoice, err, toRemove
		} else {
			level++
			//fmt.Printf("remote node, go to next level: %v\n", level)
			t.mutex.Unlock()
			node, err, rpcRemove := bestChoice.GetNextHopRPC(id, level)
			t.mutex.Lock()
			if err != nil {
				fmt.Println("11")
				t.mutex.Unlock()
				toRemove.Add(bestChoice)
				t.mutex.Lock()
				fmt.Printf("error level now is: %v\n", level)
				t.mutex.Unlock()
				t.Remove(bestChoice)
				t.mutex.Lock()
				level--
				t.mutex.Unlock()
				node, err, rpcRemove = t.GetNextHop(id, level)
				rpcRemove.AddAll(toRemove.Nodes())
				t.mutex.Lock()
				t.mutex.Unlock()
				return node, err, toRemove
			} else {
				toRemove.AddAll(rpcRemove.Nodes())
			}
		}
	}
	t.mutex.Unlock()
	return bestChoice, err, toRemove
}
