# Tapestry

## Discription about our Test

In *TestSampleTapestrySetup* test, we test our findRoot function, GetNextHop function and RemoveBadNodes function. When a node is killed, our function can remove the bad node from routing table and backpointer successfully.

In *TestSampleTapestrySearch* test, we test our Store function and get function and we can get the object correctly.

In *TestSampleTapestryAddNodes* test, we can add one node successfully every time.

*In addition to the three provided tests, we've also written another 7 tests.*

In *TestKill* test, a node can make a graceful leave successfully, and both a backpointer table and routing table are printed.

In *TestRoutingTable* test, we open the debug mode and test our slot. When we add more than three nodes in the same slot, the slot can handle the situation correctly. A routing table is printed.

In *TestLocationMap* test, we test several functions in location map and BlobStore

In *TestConn* test, we test several connect functions.

In *TestId* test, we test several id type conversions.

In *TestTapestryRPC* test, we store some words in a certain node and we can find the node successfully by calling lookup function.

## Test coverage
The overall coverage of our test is: 72.3%

## Bugs
We can add the badnode to toRemote in GetNextHop, but it failed to return to findROOT. It's the only bug we have.
