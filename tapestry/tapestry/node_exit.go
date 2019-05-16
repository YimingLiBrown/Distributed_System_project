/*
 *  Brown University, CS138, Spring 2019
 *
 *  Purpose: Defines functions for a node leaving the Tapestry mesh, and
 *  transferring its stored locations to a new node.
 */

package tapestry

import "fmt"

// Uncomment for xtrace
// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"

// Kill this node without gracefully leaving the tapestry.
func (local *Node) Kill() {
	local.blobstore.DeleteAll()
	local.server.Stop()
}

// Function to gracefully exit the Tapestry mesh.
//
// - Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
// - If possible, give each backpointer a suitable alternative node from our routing table
func (local *Node) Leave() (err error) {
	// TODO: students should implement this

	replacement := new(RemoteNode)
	for i := 0; i < DIGITS; i++ {
		backpointers, err := local.GetBackpointers(local.node, i)
		if err != nil {
			return fmt.Errorf("error when getting backpointers")
		}
		for _, bp := range backpointers {
			reps := local.table.GetLevel(i + 1)
			if len(reps) > 0 {
				*replacement = reps[0]
				REP := *replacement
				if REP.Id.String() == local.node.Id.String() {
					bp.NotifyLeaveRPC(local.node, nil)
				} else {
					bp.NotifyLeaveRPC(local.node, replacement)
				}
			} else {
				bp.NotifyLeaveRPC(local.node, nil)
			}
		}
	}
	local.blobstore.DeleteAll()
	go local.server.GracefulStop()
	return
}

// Another node is informing us of a graceful exit.
// - Remove references to the from node from our routing table and backpointers
// - If replacement is not nil, add replacement to our routing table
func (local *Node) NotifyLeave(from RemoteNode, replacement *RemoteNode) (err error) {
	// TODO: students should implement this

	if local.table.Remove(from) {
		Debug.Printf("Removed %v from routing table\n", from)
	}
	if local.backpointers.Remove(from) {
		Debug.Printf("Removed %v from backpointers\n", from)
	}
	emptyRemoteNode := RemoteNode{}
	if (replacement != nil) && (*replacement != emptyRemoteNode) {
		err = local.addRoute(*replacement)
		if err != nil {
			return fmt.Errorf("err when adding route")
		}
	}

	return err

}
