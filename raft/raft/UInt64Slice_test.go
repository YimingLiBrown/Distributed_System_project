package raft

import (
	"fmt"
	"testing"
)

func TestUInt64Slice(t *testing.T) {
	// Out.Println("TestUInt64Slice start")
	suppressLoggers()
	var set UInt64Slice
	set = append(set, uint64(1), uint64(2), uint64(3), uint64(4))
	length := set.Len()
	fmt.Printf("length of the set: %v\n", length)
	set.Swap(0, 1)
	fmt.Printf("set after the swap: %v\n", set)
	isLess := set.Less(0, length-1)
	fmt.Printf("isLess: %v\n", isLess)
	// Out.Println("TestUInt64Slice done")
	// time.Sleep(time.Second * WAIT_PERIOD)
}
