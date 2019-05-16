package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestReadStableState(t *testing.T) {
	// suppressLoggers()
	// cluster, err := createTestCluster([]int{5001, 5002, 5003, 5004, 5005})
	// defer cleanupCluster(cluster)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// time.Sleep(time.Second * WAIT_PERIOD)
	// leader, _ := findLeader(cluster)
	// fmt.Printf("leader is: %v\n", leader.GetRemoteSelf().GetId())
	//
	// stablestate, err := ReadStableState(&leader.raftMetaFd)
	// // stablestate, err := readStableStateEntry(leader.raftLogFd.fd, int(leader.raftLogFd.size))
	// if err != nil {
	// 	fmt.Printf("something wrong in this function: %v\n", err)
	// 	// t.Fatal(err)
	// }
	// fmt.Printf("leader.raftLogFd: %v\n", leader.raftMetaFd)
	// fmt.Printf("stabelstate: %v\n", stablestate)
	// // func readStableStateEntry(f *os.File, size int) (*StableState, error) {
	// // 	b := make([]byte, size)
	// // 	leSize, err := f.Read(b)
	// // 	if err != nil {
	// // 		return nil, err
	// // 	}
	// // 	if leSize != size {
	// // 		panic("The stable state log may be corrupt, cannot proceed")
	// // 	}
	// //
	// // 	buff := bytes.NewBuffer(b)
	// // 	var ss StableState
	// // 	dataDecoder := gob.NewDecoder(buff)
	// // 	err = dataDecoder.Decode(&ss)
	// // 	if err != nil {
	// // 		return nil, err
	// // 	}
	// //
	// // 	return &ss, nil
	// // }

}

func TestOpenRaftLogForWrite(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 7
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * WAIT_PERIOD)
	leader, _ := findLeader(cluster)
	fmt.Printf("leader is: %v\n", leader.GetRemoteSelf().GetId())
	err1 := openRaftLogForWrite(&leader.raftLogFd)
	if err1 != nil {
		t.Fatal(err1)
	}
	// time.Sleep(time.Second * WAIT_PERIOD)
}

// return: <nil>
// --- FAIL: TestReadRaftLog (6.81s)
//     disk_unit_test.go:82: EOF
func TestReadRaftLog(t *testing.T) {
	// Out.Println("TestReadRaftLog start")
	suppressLoggers()
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * WAIT_PERIOD)
	leader, _ := findLeader(cluster)
	fmt.Printf("leader is: %v\n", leader.GetRemoteSelf().GetId())
	logcache, err1 := ReadRaftLog(&leader.raftLogFd)
	if err1 != nil {
		// t.Fatal(err1)
		fmt.Printf("err: %v\n", err)
	}
	fmt.Printf("log cache: %v\n", logcache)

	// Out.Println("TestReadRaftLog done"
	// time.Sleep(time.Second * WAIT_PERIOD)
}
