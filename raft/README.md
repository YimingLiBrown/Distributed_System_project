# Raft

## Test coverage
Our overall test coverage ranges from 64% to 70%

The coverage of **node_follower**, **node_leader** and **node_candidate** are always above 90%.

We check the coverage of each file by using the following command:


```
go test -coverprofile=cover.out
```

wait for the test to finish and then a **cover.out** file will be generated

```
go tool cover -html=cover.out
```
It will trigger a website to open.


## Tests written by ourselves

### 1. TestClientInteraction_Candidate
The client registers itself to a candidate, and the candidate should return a hint to the client. The hint should be *"it is not a leader and the election is in progress."*

### 2. TestClientInteraction_oldleader
We start an election first to find a leader and register a client to it, then stop the leader and find a new leader. Finally the old leader joins back to the cluster, and it should fall back to a follower and give a hint to the client that it is not a leader.

### 3. TestAnotherPartition
This test shows another possible partition scheme other than the given one. In this test, the two followers, A and B, are partitioned on the one side, the leader and the other two followers C and D are on the other side. So the A and B will keep converting to candidate thus incrementing their terms, at the same time we added an entry to the leader and hopefully it will be replicated to majority and it will be commited. When the system recovers from partition, the leader will fall back to follower in the first place when detecting higher term sending out heartbeats. It will then update its own term to the term of the particular candidate sends him. But followers A and B will not collect enough votes to become leader as they don't have the commited entry in their log. Also, followers C and D which were not partitioned may also become candidate, and if at the same time one of A and B also becomes a candidate, C and D will also update their term to the higher one thus enabling themselves to become leader. So, on the big picture, the new leader would be one of the three nodes which has up-to-date log entries.

### 3. logging_unit_test.go
This is a unit test for some format functions in **logging.go** which have not been used in our project.

### 4. UInt64Slice_test.go
This is a unit test for **UInt64Slice** struct. We just test its member functions by simply appending several uint64 numbers to it.

**We add *defer cleanupCluster(cluster)* command for every test function, so when each test function can work successfully, the *raftlog* folder should be no empty.**

## Some bugs

1. Our laptops are in different operating systems. For the same code, it can always pass all the tests in Mac OS when we run tests together, but it always fails in Windows (if we run the tests one by one, Windows can pass all the tests.). When we print out some staff in Windows, we think that our code is logically correct. We also find that during the sleep period, we can always print out a leader during a election successfully, but the **findleader** function sometimes fails to detect that leader and throws out an error that *no leader in the slices*. We try to add a sleep at the end of each test function, and delete the **raftlog** folder even if it's already empty. Finally it seems like the Windows machine can pass all the tests all the time. We also run our code on a CS department machine, and it can always pass all the tests just like Mac OS.

2. We find that lots of functions in **disk_logging.go** are not tested, so we write a **disk_unit_test.go**. In Windows, it throws some errors about some related functions which are not written by us, but Mac OS can pass these tests.
