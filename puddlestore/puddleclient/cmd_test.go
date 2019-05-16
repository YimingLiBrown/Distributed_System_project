package puddleclient

import (
	"fmt"
	"testing"

	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/raft/raft"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/tapestry/tapestry"
)

func TestEverything(t *testing.T) {
	raft.SuppressLoggers()
	tapestry.SuppressLoggers()

	pc := CreateClient("0.0.0.1")
	pc.Create("raft", 3)
	// time.Sleep(time.Second * 4)
	pc.Create("tapestry", 5)
	fmt.Println("**************FINISH INITIALIZATION****************")

	err := pc.Mk("f1")
	if err != nil {
		t.Error(err)
	}
	err = pc.Mkdir("d1")
	if err != nil {
		t.Error(err)
	}
	err = pc.CdRelative("d1")
	if err != nil {
		t.Error(err)
	}
	err = pc.Mkdir("d3")
	if err != nil {
		t.Error(err)
	}
	err = pc.Mk("f2")
	if err != nil {
		t.Error(err)
	}
	err = pc.CdAbsolute("/")
	if err != nil {
		t.Error(err)
	}

	err = pc.Write("f1", 0, []byte("Hllo"))
	if err != nil {
		t.Error(err)
	}

	//////////////////////////
	b, err := pc.Read("f1", 0)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("Content: %v\n", string(b))
	err = pc.Disp("f1")
	if err != nil {
		t.Error(err)
	}
	_, err = pc.Ls()
	if err != nil {
		t.Error(err)
	}
	err = pc.Mkdir("d2")
	if err != nil {
		t.Error(err)
	}
	err = pc.Rmdir("d2")
	if err != nil {
		t.Error(err)
	}
	err = pc.Rm("f1")
	if err != nil {
		t.Error(err)
	}
	err = pc.Rmdir("d1")
	if err != nil {
		t.Error(err)
	}
	_, err = pc.AddTnode("6543")
	if err != nil {
		t.Error(err)
	}
	_, err = pc.RemoveTnodes("2")
	if err != nil {
		t.Error(err)
	}
	fmt.Println("**************BELOW ARE SOME ACTIONS THAT ARE NOT PERMITTED****************")

	err = pc.CdAbsolute("/d1/d3")
	if err == nil {
		t.Errorf("Parent folder should already been deleted\n")
	}
	err = pc.CdRelative("d1/d3")
	if err == nil {
		t.Errorf("Parent folder should already been deleted\n")
	}
	_, err = pc.Read("d1/f2", 0)
	if err == nil {
		t.Errorf("Parent folder should already been deleted\n")
	}

}
