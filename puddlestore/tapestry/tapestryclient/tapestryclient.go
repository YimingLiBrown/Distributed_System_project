/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Allows third-party clients to connect to a Tapestry node (such as
 *  a web app, mobile app, or CLI that you write), and put and get objects.
 */

package tapestryclient

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/tapestry/tapestry"
)

const SALT_NUM = 3

type Client struct {
	Id   string
	node *tapestry.RemoteNode
}

// Connect to a Tapestry node
func Connect(addr string) (*Client, error) {
	node, err := tapestry.SayHelloRPC(addr, tapestry.RemoteNode{})
	if err != nil {
		tapestry.Error.Printf("Failed to make clientection to Tapestry node\n")
	}
	return &Client{node.Id.String(), &node}, err
}

// Invoke tapestry.Store on the remote Tapestry node
func (client *Client) Store(key string, value []byte) error {
	tapestry.Debug.Printf("Making remote TapestryStore call\n")
	for i := 0; i < SALT_NUM; i++ {
		skey := key + strconv.FormatInt(int64(i), 10)
		err := client.node.TapestryStoreRPC(skey, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Invoke tapestry.Lookup on a remote Tapestry node
func (client *Client) Lookup(key string) ([]*Client, error) {
	tapestry.Debug.Printf("Making remote TapestryLookup call\n")

	for i := 0; i < SALT_NUM; i++ {
		skey := key + strconv.FormatInt(int64(i), 10)
		nodes, err := client.node.TapestryLookupRPC(skey)
		if err == nil {
			clients := make([]*Client, len(nodes))
			for i, n := range nodes {
				clients[i] = &Client{n.Id.String(), &n}
			}
			return clients, err
		}
	}
	return nil, errors.New("Nothing found!")
}

// Get data from a Tapestry node. Looks up key then fetches directly.
func (client *Client) Get(key string) ([]byte, error) {
	tapestry.Debug.Printf("Making remote TapestryGet call\n")
	// Lookup the key
	var replicas []tapestry.RemoteNode
	var err error
	var skey string

	for i := 0; i < SALT_NUM; i++ {
		skey = key + strconv.FormatInt(int64(i), 10)
		replicas, err = client.node.TapestryLookupRPC(skey)
		if err == nil && len(replicas) > 0 {
			break
		}
	}

	if err != nil {
		return nil, err
	}
	if len(replicas) == 0 {
		return nil, fmt.Errorf("No replicas returned for key %v", skey)
	}

	var errs []error
	for _, replica := range replicas {
		blob, err := replica.BlobStoreFetchRPC(skey)
		if err != nil {
			errs = append(errs, err)
		}
		if blob != nil {
			return *blob, nil
		}
	}

	return nil, fmt.Errorf("Error contacting replicas, %v: %v", replicas, errs)
}
