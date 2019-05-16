package tapestry

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSampleTapestrySetup(t *testing.T) {
	fmt.Println("DEBUG: 1ST")
	tap, _ := MakeTapestries(true, "1111", "1231", "1232") //Make a tapestry with these ids
	fmt.Println("DEBUG: TAPESTRY1 MADE.")
	KillTapestries(tap[1]) //Kill off two of them.
	next, _, _ := tap[0].GetNextHop(MakeID("1232"), 0)
	if next != tap[0].node {
	}
}

func TestSampleTapestrySearch(t *testing.T) {
	tap, _ := MakeTapestries(true, "100", "456", "1234") //make a sample tap
	fmt.Println("DEBUG: TAPESTRY2 MADE.")
	tap[1].Store("look at this lad", []byte("an absolute unit"))
	result, _ := tap[0].Get("look at this lad")           //Store a KV pair and try to fetch it
	if !bytes.Equal(result, []byte("an absolute unit")) { //Ensure we correctly get our KV
		fmt.Printf("DEBUG: result: %v\n", result)
		t.Errorf("Get failed")
	}
}

func TestSampleTapestryAddNodes(t *testing.T) {
	tap, _ := MakeTapestries(true, "1", "5", "9")
	fmt.Println("DEBUG: TAPESTRY MADE.")
	node8, tap, _ := AddOne("8", tap[0].node.Address, tap) //Add some tap nodes after the initial construction
	_, tap, _ = AddOne("12", tap[0].node.Address, tap)
	next, _, _ := tap[1].table.GetNextHop(MakeID("7"), 0)
	if node8.node != next {
		t.Errorf("Addition of node failed")
	}
}

func TestKill(t *testing.T) {
	tap, _ := MakeTapestries(true, "1000", "5000", "5311", "5312", "5321")
	fmt.Println("DEBUG: TAPESTRY5 MADE.")
	tap[0].Store("look at this lad", []byte("an absolute unit"))
	tap[3].PrintBackpointers()
	tap[3].PrintRoutingTable()
	tap[2].Leave()
	tap[2].Get("look at this lad")
	tap[3].PrintBackpointers()
	tap[3].PrintRoutingTable()

}

func TestRoutingTable(t *testing.T) {
	SetDebug(true)
	tap, _ := MakeTapestries(true, "10", "53", "25", "52", "51", "27")
	fmt.Println("DEBUG: TAPESTRY6 MADE.")
	tap[0].PrintRoutingTable()
}

func TestLocationMap(t *testing.T) {

	tap, _ := MakeTapestries(true, "2345", "3456", "5678")
	fmt.Println("DEBUG: TAPESTRY7 MADE.")
	tap[1].PrintLocationMap()
	tap[1].LocationMapToString()
	tap[2].PrintBlobStore()
	tap[2].BlobStoreToString()
}

func TestConn(t *testing.T) {
	tap, _ := MakeTapestries(true, "1000", "5000", "5311", "5312", "6893")
	fmt.Println("DEBUG: TAPESTRY8 MADE.")
	makeClientConn(&tap[0].node) // Creates a new client connection to the given remote node
	makeClientConn(&tap[1].node)
	makeClientConn(&tap[2].node)
	closeAllConnections()
	makeClientConn(&tap[3].node)
	tap[3].node.RemoveClientConn()

}

func TestId(t *testing.T) {
	tap, _ := MakeRandomTapestries(int64(8), 3)
	fmt.Println("DEBUG: TAPESTRY8 MADE.")
	fmt.Printf("1: %v, 2: %v, 3: %v\n", tap[0], tap[1], tap[2])
	bt := tap[0].node.Id.bytes()
	id := idFromBytes(bt)
	randomId := RandomID()
	fmt.Printf("byte is: %v, id is: %v, random id is: %v\n", bt, id, randomId)
}

func TestTapestryRPC(t *testing.T) {
	tap, _ := MakeTapestries(true, "1000", "5000", "5311", "5312", "6893")
	tap[1].Store("look at this lad", []byte("an absolute unit"))
	node, _ := tap[1].node.TapestryLookupRPC("look at this lad")
	fmt.Printf("node: %v\n", node)
	bt := tap[2].node.Id.bytes()
	tap[0].node.TapestryStoreRPC("key", bt)
}
