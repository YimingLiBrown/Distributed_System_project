package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestLogging(t *testing.T) {
	// Out.Println("TestTestLogging start")
	suppressLoggers()
	SetDebug(true)
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * WAIT_PERIOD)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	formatstate := cluster[0].FormatState()
	fmt.Printf("formatstate: %v\n", formatstate)
	fmtlogcache := cluster[1].FormatLogCache()
	fmt.Printf("formatlogcache: %v\n", fmtlogcache)
	fmtnodelist := leader.FormatNodeListIds(leader.Id)
	fmt.Printf("formatnodelist: %v\n", fmtnodelist)

	// Out.Println("TestTestLogging done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}
