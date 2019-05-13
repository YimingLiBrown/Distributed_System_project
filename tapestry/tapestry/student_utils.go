package tapestry

import (
	"math/rand"
	"strconv"
	"sync"
	"time"
)

//Create a mesh of the all the tapestry nodes for use in other functions
var tapestriesByAddress map[string]*Node = make(map[string]*Node)
var tapestryMapMutex *sync.Mutex = &sync.Mutex{}

//Generates an ID of length 40 from some hex
//string ("4", "987654321", "beef", etc.)
func MakeID(stringID string) ID {
	var id ID

	for i := 0; i < DIGITS && i < len(stringID); i++ {
		d, err := strconv.ParseInt(stringID[i:i+1], 16, 0)
		if err != nil {
			return id
		}
		id[i] = Digit(d)
	}
	for i := len(stringID); i < DIGITS; i++ {
		id[i] = Digit(0)
	}

	return id
}

//Given some number of strings, turns them into IDs, and then tapestry nodes.
//Then, form them into a tapestry network if connectThem=true. Else,
//make several unconnected networks.
func MakeTapestries(connectThem bool, ids ...string) ([]*Node, error) {
	tapestries := make([]*Node, 0, len(ids))
	for i := 0; i < len(ids); i++ {
		connectTo := ""
		if i > 0 && connectThem {
			connectTo = tapestries[0].node.Address
			//fmt.Printf("DEBUG: connectTo: %v\n", connectTo)
		}
		//fmt.Println("DEBUG: calling start")
		t, err := start(MakeID(ids[i]), 0, connectTo)
		if err != nil {
			return tapestries, err
		}
		registerCachedTapestry(t)
		tapestries = append(tapestries, t)
		time.Sleep(10 * time.Millisecond)
	}
	return tapestries, nil
}
func KillTapestries(ts ...*Node) {
	unregisterCachedTapestry(ts...)
	for _, t := range ts {
		t.Kill()
	}
}

//Helper function to register nodes into our tapestry mesh
func registerCachedTapestry(tapestry ...*Node) {
	tapestryMapMutex.Lock()
	defer tapestryMapMutex.Unlock()
	for _, t := range tapestry {
		tapestriesByAddress[t.node.Address] = t
	}
}

//Helper function used to remove tapestry nodes from our mesh
func unregisterCachedTapestry(tapestry ...*Node) {
	tapestryMapMutex.Lock()
	defer tapestryMapMutex.Unlock()
	for _, t := range tapestry {
		delete(tapestriesByAddress, t.node.Address)
	}
}

//Adds a node to the tapestry network.
//ida - the string you wish to convert to an ID, and then into a tap node
//addr - address of a node in the current network to connect to.
//tap - list of the current nodes in the tap network. Returns this list plus the new node
func AddOne(ida string, addr string, tap []*Node) (t1 *Node, tapNew []*Node, err error) {
	t1, err = start(MakeID(ida), 0, addr)
	if err != nil {
		return nil, tap, err
	}
	registerCachedTapestry(t1)
	tapNew = append(tap, t1)
	time.Sleep(1000 * time.Millisecond) //Wait for availability of new node
	return
}

//Makes a random tapestry mesh given a seed and size
func MakeRandomTapestries(seed int64, count int) ([]*Node, error) {
	r := rand.New(rand.NewSource(seed))

	ts := make([]*Node, 0, count)

	for i := 0; i < count; i++ {
		connectTo := ""
		if i > 0 {
			connectTo = ts[0].node.Address
		}
		t, err := start(IntToID(r.Int()), 0, connectTo)
		if err != nil {
			return ts, err
		}
		registerCachedTapestry(t)
		ts = append(ts, t)
		time.Sleep(10 * time.Millisecond)
	}

	return ts, nil
}

//Helper function to transform an integer to an ID
func IntToID(x int) ID {
	var id ID
	for i := range id {
		id[i] = Digit(x % BASE)
		x = x / BASE
	}
	return id
}
