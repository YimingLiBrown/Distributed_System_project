package puddleclient

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/puddlestore"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/raft/hashmachine"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/raft/raft"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/raft/raftclient"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/tapestry/tapestry"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/tapestry/tapestryclient"
	"github.com/google/uuid"
	"github.com/samuel/go-zookeeper/zk"
)

type PuddleClient struct {
	LocalAddr string
	RClient   *raftclient.Client
	TClient   *tapestryclient.Client
	RNode     []raft.RemoteNode
	TNode     []tapestry.Node
	Zk        Zookeeper
	Path      string            //obsolute path
	Table     map[string]string //map[path]aguid
	RFLAG     bool
	TFLAG     bool
}

type Zookeeper struct {
	// Path     map[string]string //map[addr]path
	Conn     *zk.Conn
	RootPath string
	Children []string
}

func CreateClient(addr string) (pc *PuddleClient) {
	//fmt.Println("create a puddlestore client")
	pc = new(PuddleClient)
	pc.LocalAddr = addr
	pc.RClient = &raftclient.Client{}
	pc.TClient = &tapestryclient.Client{}
	pc.RNode = make([]raft.RemoteNode, 0)
	pc.TNode = make([]tapestry.Node, 0)
	pc.Zk = Zookeeper{}
	pc.Zk.RootPath = "/"
	pc.Path = ""
	pc.Table = make(map[string]string)
	pc.RFLAG = false
	pc.TFLAG = false
	conn, _, err := zk.Connect([]string{"0.0.0.0"}, time.Second*5)
	if err != nil {
		fmt.Printf("fail to connect the zookeeper to the host： %v\n", err)
		return nil
	}
	pc.Zk.Conn = conn
	isRExit, _, err := conn.Exists("/raft")
	if err != nil {
		fmt.Printf("fail to check the path： %v\n", err)
	}
	isTExit, _, err := conn.Exists("/tapestry")
	if err != nil {
		fmt.Printf("fail to check the path： %v\n", err)
	}
	// fmt.Printf("is exist: %v\n", isExit)
	Rchildren, _, err := conn.Children("/raft")
	fmt.Printf("raft children: %v\n", Rchildren)

	Tchildren, _, err := conn.Children("/tapestry")
	fmt.Printf("tap children: %v\n", Tchildren)

	if isRExit && isTExit {
		Rchildren, _, err := conn.Children("/raft")
		if err != nil {
			return nil
		}
		if len(Rchildren) != 0 && len(Tchildren) != 0 {
			///
			rAddr, _, err := conn.Get("/raft/" + Rchildren[0])
			if err != nil {
				return nil
			}
			pc.RClient, err = raftclient.Connect(string(rAddr))
			if err != nil {
				fmt.Printf("raft client connect err: %v\n", err)
				return nil
			}

			// tAddrs, err := pc.GetTClients()
			// if err != nil {
			// 	return nil
			// }
			// pc.TClient, err = tapestryclient.Connect(tAddrs[0])
			// if err != nil {
			// 	fmt.Printf("tap client connect err: %v\n", err)
			// 	return nil
			// }
			// pc.TClient.lo
			pc.Path = "/"
			pc.Zk.Conn = conn

			//-----------------------------------------------//
			aguid := uuid.New().String()
			// hasher := md5.New()
			// hasher.Write([]byte(pc.Path))
			// aguid := hex.EncodeToString(hasher.Sum(nil))
			//create 	1.indirectblock 2.Inode
			//create datablocks and vguid
			dbList := make([]string, 0)
			ib := puddlestore.IndirectBlock{ //initialize indirectblock
				List: dbList,
			}
			ibVGUID := uuid.New().String() //generate indirectblock vguid
			ibBYTE, _ := puddlestore.EncodeIndirectBlock(ib)
			addrs, _ := pc.GetTClients()
			for _, addr := range addrs {
				tclient, err := tapestryclient.Connect(addr)
				if err != nil {
					fmt.Printf("fail to connect to a client: %v\n", err)
					return nil
				}
				pc.TClient = tclient
				pc.TClient.Store(ibVGUID, ibBYTE)
			}
			// fmt.Println("---------------------------------------")
			inode := puddlestore.Inode{ //initialize Inode
				AGUID:         aguid,
				Name:          "/",
				Type:          puddlestore.DIR,
				IndirectBlock: ibVGUID,
				Size:          uint64(0), //uint64(len(ib.List)),
			}
			inVGUID, _ := pc.CreateVguid(aguid) //generate inode vguid through raft
			inBYTE, _ := puddlestore.EncodeInode(inode)
			// tclients, _ = pc.GetTClients()
			// for i := 0; i < 3; i++ {
			// 	pc.TClient = tclients[i]
			// 	pc.TClient.Store(inVGUID, inBYTE)
			// }
			addrs, _ = pc.GetTClients()
			for _, addr := range addrs {
				tclient, err := tapestryclient.Connect(addr)
				if err != nil {
					fmt.Printf("fail to connect to a client: %v\n", err)
					return nil
				}
				pc.TClient = tclient
				pc.TClient.Store(inVGUID, inBYTE)
			}

			pc.Table[pc.Path] = aguid
			//-------------------------------------------------//
		}

	}

	return pc
}

func CreateZkp() (zkp Zookeeper) {
	// zkp.Path = make(map[string]string, 0)
	zkp.Conn = new(zk.Conn)
	zkp.RootPath = ""
	zkp.Children = make([]string, 0)
	return
}

func (pc *PuddleClient) Create(nodetype string, num int) ([]string, error) {
	if nodetype == "raft" {
		children, conn, err := Zkregister(pc.Zk.RootPath, nodetype)
		pc.Zk.Children = children
		pc.Zk.Conn = conn
		if err != nil {
			return pc.Zk.Children, err
		}
		//create a cluster
		config := raft.DefaultConfig()
		config.ClusterSize = num
		rnodes, err := raft.CreateLocalCluster(config)
		if err != nil {
			fmt.Printf("Create local cluster error: %v\n ", err)
			return pc.Zk.Children, err
		}
		pc.RNode = rnodes[0].GetNodeList()
		//wait to find leader
		time.Sleep(time.Second * 4)

		pc.RClient, err = raftclient.Connect(pc.RNode[0].GetAddr())
		if err != nil {
			fmt.Printf("raft client connect err: %v\n", err)
			return pc.Zk.Children, err
		}

		pc.RFLAG = true
	} else if nodetype == "tapestry" {
		var nodes []tapestry.Node
		for i := 0; i < num; i++ {
			node, err := tapestry.Start(2000+i, "")
			nodes = append(nodes, *node)
			if err != nil {
				return nil, err
			}
		}
		pc.TNode = nodes
		pc.TFLAG = true
	}
	if pc.RFLAG && pc.TFLAG {
		pc.Path = "/"
		//aguid := uuid.New().String()
		hasher := md5.New()
		hasher.Write([]byte(pc.Path))
		aguid := hex.EncodeToString(hasher.Sum(nil))
		//create 	1.indirectblock 2.Inode
		//create datablocks and vguid
		dbList := make([]string, 0)
		ib := puddlestore.IndirectBlock{ //initialize indirectblock
			List: dbList,
		}
		ibVGUID := uuid.New().String() //generate indirectblock vguid
		ibBYTE, _ := puddlestore.EncodeIndirectBlock(ib)
		addrs, _ := pc.GetTClients()
		for _, addr := range addrs {
			tclient, err := tapestryclient.Connect(addr)
			if err != nil {
				fmt.Printf("fail to connect to a client: %v\n", err)
				return pc.Zk.Children, err
			}
			pc.TClient = tclient
			pc.TClient.Store(ibVGUID, ibBYTE)
		}
		// fmt.Println("---------------------------------------")
		inode := puddlestore.Inode{ //initialize Inode
			AGUID:         aguid,
			Name:          "/",
			Type:          puddlestore.DIR,
			IndirectBlock: ibVGUID,
			Size:          uint64(0), //uint64(len(ib.List)),
		}
		inVGUID, _ := pc.CreateVguid(aguid) //generate inode vguid through raft
		inBYTE, _ := puddlestore.EncodeInode(inode)
		// tclients, _ = pc.GetTClients()
		// for i := 0; i < 3; i++ {
		// 	pc.TClient = tclients[i]
		// 	pc.TClient.Store(inVGUID, inBYTE)
		// }
		addrs, _ = pc.GetTClients()
		for _, addr := range addrs {
			tclient, err := tapestryclient.Connect(addr)
			if err != nil {
				fmt.Printf("fail to connect to a client: %v\n", err)
				return pc.Zk.Children, err
			}
			pc.TClient = tclient
			pc.TClient.Store(inVGUID, inBYTE)
		}

		pc.Table[pc.Path] = aguid
	}

	return pc.Zk.Children, nil
}

func Zkregister(path string, nodetype string) ([]string, *zk.Conn, error) {
	// fmt.Printf("path: %v, pnode: %v, nodetype: %vv\n", path, pnode, nodetype)
	zkp := CreateZkp()
	children := make([]string, 0)
	var hosts = []string{"0.0.0.0"}
	//make a zookeeper connection
	conn, _, err := zk.Connect(hosts, time.Second*5)
	if err != nil {
		fmt.Println(err)
		return children, zkp.Conn, err
	}
	fmt.Println("zkp connection is successful")

	acls := zk.WorldACL(zk.PermAll) //permission for all operations
	if nodetype == "raft" {
		conn.Create("/raft", []byte{}, int32(0), acls)
		children, _, _ = conn.Children("/raft")
		fmt.Printf("children: %v\n", children)
		zkp.Conn = conn
		return children, zkp.Conn, err
	} else if nodetype == "tapestry" {
		conn.Create("/tapestry", []byte{}, int32(0), acls)
		children, _, _ = conn.Children("/tapestry")
		fmt.Printf("tapestry children: %v\n", children)
		zkp.Conn = conn
		return children, zkp.Conn, err
	} else {
		return children, zkp.Conn, errors.New("invalid node type")
	}
	// return children, zkp.Conn, err
}

// make a file(type:1) in the current path
// is just initialize inode, indirectblock and datablock
func (pc *PuddleClient) Mk(filename string) (err error) {
	//generate a unique aguid randomly
	var pathnow string
	if pc.Path == "/" {
		pathnow = pc.Path + filename
	} else {
		pathnow = pc.Path + "/" + filename
	}

	ok := pc.CheckExist(filename)
	if ok {
		return errors.New("File exists!")
	}
	//aguid := uuid.New().String()
	hasher := md5.New()
	hasher.Write([]byte(pathnow))
	aguid := hex.EncodeToString(hasher.Sum(nil))
	//create 	1.datablocks 2.indirectblock 3.Inode
	//create datablocks and vguid
	db := puddlestore.DataBlock{ //initialize datablock
		Data: make([]byte, 0),
	}
	dbVGUID := uuid.New().String() //generate datablock vguid
	dbBYTE, _ := puddlestore.EncodeDataBlock(db)
	// tclients, _ := pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(dbVGUID, dbBYTE) // store datablock into tapestry
	// }
	addrs, _ := pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(dbVGUID, dbBYTE)
	}

	dbList := make([]string, 0)
	dbList = append(dbList, dbVGUID)
	ib := puddlestore.IndirectBlock{ //initialize indirectblock
		List: dbList,
	}
	ibVGUID := uuid.New().String() //generate indirectblock vguid
	ibBYTE, _ := puddlestore.EncodeIndirectBlock(ib)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(ibVGUID, ibBYTE)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(ibVGUID, ibBYTE)
	}

	inode := puddlestore.Inode{ //initialize Inode
		AGUID:         aguid,
		Name:          filename,
		Type:          puddlestore.FILE,
		IndirectBlock: ibVGUID,
		Size:          uint64(len(db.Data)),
	}
	inVGUID, _ := pc.CreateVguid(aguid) //generate inode vguid through raft
	inBYTE, _ := puddlestore.EncodeInode(inode)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(inVGUID, inBYTE)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(inVGUID, inBYTE)
	}

	path, _ := pc.Pwd()
	//dirAGUID := pc.Table[path]                             //get the folder inode's aguid
	hasher2 := md5.New()
	hasher2.Write([]byte(path))
	dirAGUID := hex.EncodeToString(hasher2.Sum(nil))
	dirVGUID, _ := pc.ReadVguid(dirAGUID)                  // get the inode's vguid through raft
	dirINBYTE, _ := pc.TClient.Get(dirVGUID)               //get the inode from tapestry
	dirIN, _ := puddlestore.DecodeInode(dirINBYTE)         //decode
	dirIBVGUID := dirIN.IndirectBlock                      //get indirectblock's vguid through inode
	dirIBBYTE, _ := pc.TClient.Get(dirIBVGUID)             //get indirectblock from tapestry
	dirIB, _ := puddlestore.DecodeIndirectBlock(dirIBBYTE) //decode
	dirIB.List = append(dirIB.List, aguid)                 //connect the file inode's aguid with dierectory's indirectblock

	newDirIBVGUID := uuid.New().String() //update the dir info in tapestry
	newdirIBBYTE, _ := puddlestore.EncodeIndirectBlock(dirIB)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newDirIBVGUID, newdirIBBYTE)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newDirIBVGUID, newdirIBBYTE)
	}

	dirIN.IndirectBlock = newDirIBVGUID
	newInodeBYTE, _ := puddlestore.EncodeInode(dirIN)
	newInodeVGUID, _ := pc.CreateVguid(dirAGUID)
	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newInodeVGUID, newInodeBYTE)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newInodeVGUID, newInodeBYTE)
	}
	// pc.TClient.Store(newInodeVGUID, newInodeBYTE)
	var newpath string
	if path == "/" {
		newpath = path + filename
	} else {
		newpath = path + "/" + filename //record the file inod's path and aguid
	}
	pc.Table[newpath] = inode.AGUID
	return err
}

// create a vguid with a given aguid
func (pc *PuddleClient) CreateVguid(aguid string) (vguid string, err error) {
	//get vguid through raft hashMachine
	reply, err := pc.RClient.SendRequest(hashmachine.HASH_CHAIN_CREATE, []byte(aguid))
	if err != nil {
		fmt.Printf("fail to send MK request to raft: %v\n", err)
		return "", err
	}
	vguid = reply.GetResponse()
	return vguid, err
}

func (pc *PuddleClient) ReadVguid(aguid string) (vguid string, err error) {
	//get vguid through raft hashMachine
	reply, err := pc.RClient.SendRequest(hashmachine.HASH_CHAIN_READ, []byte(aguid))
	if err != nil {
		fmt.Printf("fail to send MK request to raft: %v\n", err)
		return "", err
	}
	vguid = reply.GetResponse()
	return vguid, err
}

func (pc *PuddleClient) DeleteVguid(aguid string) (err error) {
	//get vguid through raft hashMachine
	_, err = pc.RClient.SendRequest(hashmachine.HASH_CHAIN_DELETE, []byte(aguid))
	if err != nil {
		fmt.Printf("fail to send MK request to raft: %v\n", err)
		return err
	}

	return err
}

// make a directory(type:0) in the current path
// is just initialize inode, indirectblock and datablock
func (pc *PuddleClient) Mkdir(dirname string) (err error) {
	//var ok bool
	var pathnow string
	if pc.Path == "/" {
		pathnow = pc.Path + dirname
	} else {
		pathnow = pc.Path + "/" + dirname
	}

	ok := pc.CheckExist(dirname)
	if ok {
		return errors.New("dir exists!")
	}

	//generate a unique aguid randomly
	//aguid := uuid.New().String()
	hasher := md5.New()
	hasher.Write([]byte(pathnow))
	aguid := hex.EncodeToString(hasher.Sum(nil))
	// fmt.Printf("aguid: %v\n", aguid)
	//create 	1.indirectblock 2.Inode
	//create datablocks and vguid
	dbList := make([]string, 0)
	ib := puddlestore.IndirectBlock{ //initialize indirectblock
		List: dbList,
	}
	ibVGUID := uuid.New().String() //generate indirectblock vguid
	ibBYTE, _ := puddlestore.EncodeIndirectBlock(ib)
	// tclients, _ := pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(ibVGUID, ibBYTE)
	// }
	addrs, _ := pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(ibVGUID, ibBYTE)
	}

	inode := puddlestore.Inode{ //initialize Inode
		AGUID:         aguid,
		Name:          dirname,
		Type:          puddlestore.DIR,
		IndirectBlock: ibVGUID,
		Size:          uint64(0), //uint64(len(ib.List)),
	}
	inVGUID, _ := pc.CreateVguid(aguid) //generate inode vguid through raft
	inBYTE, _ := puddlestore.EncodeInode(inode)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(inVGUID, inBYTE)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(inVGUID, inBYTE)
	}

	path, _ := pc.Pwd()
	//dirAGUID := pc.Table[path]                             //get the folder inode's aguid
	hasher2 := md5.New()
	hasher2.Write([]byte(path))
	dirAGUID := hex.EncodeToString(hasher2.Sum(nil))
	dirVGUID, _ := pc.ReadVguid(dirAGUID)                  // get the inode's vguid through raft
	dirINBYTE, _ := pc.TClient.Get(dirVGUID)               //get the inode from tapestry
	dirIN, _ := puddlestore.DecodeInode(dirINBYTE)         //decode
	dirIBVGUID := dirIN.IndirectBlock                      //get indirectblock's vguid through inode
	dirIBBYTE, _ := pc.TClient.Get(dirIBVGUID)             //get indirectblock from tapestry
	dirIB, _ := puddlestore.DecodeIndirectBlock(dirIBBYTE) //decode
	dirIB.List = append(dirIB.List, aguid)                 //connect the file inode's aguid with dierectory's indirectblock

	newDirIBVGUID := uuid.New().String() //update the dir info in tapestry
	newDirIbBYTE, _ := puddlestore.EncodeIndirectBlock(dirIB)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newDirIBVGUID, newDirIbBYTE)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newDirIBVGUID, newDirIbBYTE)
	}

	dirIN.IndirectBlock = newDirIBVGUID
	newDirINVGUID, _ := pc.CreateVguid(dirAGUID)
	newDirINBYTE, _ := puddlestore.EncodeInode(dirIN)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newDirINVGUID, newDirINBYTE)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newDirINVGUID, newDirINBYTE)
	}

	var newpath string
	if path == "/" {

		newpath = path + dirname
	} else {
		newpath = path + "/" + dirname //record the file inod's path and aguid
	} //record the file inod's path and aguid
	pc.Table[newpath] = inode.AGUID
	return err

}

//remove a file in the current path
func (pc *PuddleClient) Rm(filename string) (err error) {

	//var ok bool
	var filePath string
	if pc.Path == "/" {
		filePath = pc.Path + filename
	} else {
		filePath = pc.Path + "/" + filename
	}

	ok := pc.CheckExist(filename)
	if !ok {
		return errors.New("File doesn't exist!")
	}
	addrs, _ := pc.GetTClients()
	tclient, err := tapestryclient.Connect(addrs[0])
	if err != nil {
		fmt.Printf("fail to connect to a client: %v\n", err)
		return err
	}
	pc.TClient = tclient

	//fileAGUID := pc.Table[filePath]
	hasher := md5.New()
	hasher.Write([]byte(filePath))
	fileAGUID := hex.EncodeToString(hasher.Sum(nil))

	//dirINAGUID := pc.Table[pc.Path]
	hasher2 := md5.New()
	hasher2.Write([]byte(pc.Path))
	dirINAGUID := hex.EncodeToString(hasher2.Sum(nil))
	dirINVGUID, _ := pc.ReadVguid(dirINAGUID)
	dirINBYTE, _ := pc.TClient.Get(dirINVGUID)
	dirIN, _ := puddlestore.DecodeInode(dirINBYTE) //get the inode of the current dir path

	dirIBVGUID := dirIN.IndirectBlock
	dirIBBYTE, _ := pc.TClient.Get(dirIBVGUID)
	dirIB, _ := puddlestore.DecodeIndirectBlock(dirIBBYTE) //get the indirectblock of the directory
	for i, id := range dirIB.List {
		if id == fileAGUID {
			dirIB.List = append(dirIB.List[:i], dirIB.List[i+1:]...)
			delete(pc.Table, filePath)
			break
		}
	}
	newIBByte, _ := puddlestore.EncodeIndirectBlock(dirIB)
	newIBVguid := uuid.New().String()

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newIBVguid, newIBByte)
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newIBVguid, newIBByte)
	}

	dirIN.IndirectBlock = newIBVguid
	newINByte, _ := puddlestore.EncodeInode(dirIN)
	newINVguid, _ := pc.CreateVguid(dirINAGUID)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newINVguid, newINByte)
	//
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newINVguid, newINByte)
	}

	pc.DeleteVguid(fileAGUID)
	return err
}

func (pc *PuddleClient) Rmdir(dirname string) (err error) {
	//var ok bool
	var dirPath string

	if pc.Path == "/" {
		dirPath = pc.Path + dirname
	} else {
		dirPath = pc.Path + "/" + dirname
	}
	ok := pc.CheckExist(dirname)
	if !ok {
		return errors.New("Dir doesn't exist!")
	}
	// tclients, _ := pc.GetTClients()
	// pc.TClient = tclients[0]

	addrs, _ := pc.GetTClients()
	tclient, err := tapestryclient.Connect(addrs[0])
	if err != nil {
		fmt.Printf("fail to connect to a client: %v\n", err)
		return err
	}
	pc.TClient = tclient

	prevDirPath := pc.Path
	//dirAGUID := pc.Table[dirPath]
	hasher := md5.New()
	hasher.Write([]byte(dirPath))
	dirAGUID := hex.EncodeToString(hasher.Sum(nil))

	//prevDirINAGUID := pc.Table[prevDirPath] //previous directory inode AGUID
	hasher2 := md5.New()
	hasher2.Write([]byte(prevDirPath))
	prevDirINAGUID := hex.EncodeToString(hasher2.Sum(nil))

	prevDirINVGUID, _ := pc.ReadVguid(prevDirINAGUID)
	prevDirINBYTE, _ := pc.TClient.Get(prevDirINVGUID)
	prevDirIN, _ := puddlestore.DecodeInode(prevDirINBYTE) //get the inode of the current dir path

	prevDirIBVGUID := prevDirIN.IndirectBlock
	prevDirIBBYTE, _ := pc.TClient.Get(prevDirIBVGUID)
	prevDirIB, _ := puddlestore.DecodeIndirectBlock(prevDirIBBYTE) //get the indirectblock of the directory

	for i, id := range prevDirIB.List {
		if id == dirAGUID {
			prevDirIB.List = append(prevDirIB.List[:i], prevDirIB.List[i+1:]...)
			break
		}
	}
	newIBByte, _ := puddlestore.EncodeIndirectBlock(prevDirIB)
	newIBVguid := uuid.New().String()

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newIBVguid, newIBByte)
	//
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newIBVguid, newIBByte)
	}

	prevDirIN.IndirectBlock = newIBVguid
	newINByte, _ := puddlestore.EncodeInode(prevDirIN)
	newINVguid, _ := pc.CreateVguid(prevDirINAGUID)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newINVguid, newINByte)
	//
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newINVguid, newINByte)
	}

	delete(pc.Table, dirPath)
	pc.DeleteVguid(dirAGUID)
	return err

}

func (pc *PuddleClient) Read(filename string, location int) (buff []byte, err error) {
	buff = make([]byte, 0)
	err = pc.RWFilehelp(filename)
	if err != nil {
		return buff, err
	}
	//var ok bool

	ok := pc.CheckExist(filename)
	if !ok {
		return buff, errors.New("File doesn't exist!")
	}
	if pc.Path == "/" {
		filename = pc.Path + filename
		// path = pc.Path + filename
	} else {
		filename = pc.Path + "/" + filename
		// path = pc.Path"/"+filename
	}
	// tclients, _ := pc.GetTClients()
	// pc.TClient = tclients[0]
	addrs, _ := pc.GetTClients()
	tclient, err := tapestryclient.Connect(addrs[0])
	if err != nil {
		fmt.Printf("fail to connect to a client: %v\n", err)
		return buff, err
	}
	pc.TClient = tclient
	//aguid := pc.Table[filename]
	hasher := md5.New()
	hasher.Write([]byte(filename))
	aguid := hex.EncodeToString(hasher.Sum(nil))

	vguid, _ := pc.ReadVguid(aguid)
	inodeB, _ := pc.TClient.Get(vguid)
	inode, _ := puddlestore.DecodeInode(inodeB)
	if inode.Type == puddlestore.DIR {
		return buff, errors.New("Not a file!")
	}
	ibVguid := inode.IndirectBlock
	ibByte, _ := pc.TClient.Get(ibVguid)
	ib, _ := puddlestore.DecodeIndirectBlock(ibByte)
	idList := ib.List
	var content string
	for _, vguid := range idList {
		tmpByte, _ := pc.TClient.Get(vguid)
		tmp, _ := puddlestore.DecodeDataBlock(tmpByte)
		content += string(tmp.Data)
	}

	if location == 0 || location < len(content) {
		temp := content[location:]
		buff = []byte(temp)
		return buff, nil
	} else {
		return buff, errors.New("Out of range!")
	}
}

func (pc *PuddleClient) Write(filename string, location int, content []byte) (err error) {
	err = pc.RWFilehelp(filename)
	if err != nil {
		fmt.Println("RWFILEHELP ERR")
		return err
	}
	//var ok bool
	var path1 string

	ok := pc.CheckExist(filename)
	if !ok {
		return errors.New("File doesn't exist!")
	}

	if pc.Path == "/" {
		path1 = pc.Path + filename
		// path = pc.Path + filename
	} else {
		path1 = pc.Path + "/" + filename
		// path = pc.Path"/"+filename
	}
	// tclients, _ := pc.GetTClients()
	// pc.TClient = tclients[0]
	// addrs, err := pc.GetTClients()
	// if err != nil {
	// 	fmt.Printf("GETTCLIENTS ERR: %v\n", err)
	// 	return err
	// }
	// tclient, err := tapestryclient.Connect(addrs[0])
	// if err != nil {
	// 	fmt.Printf("fail to connect to a client: %v\n", err)
	// 	return err
	// }
	// pc.TClient = tclient

	buff, err := pc.Read(filename, 0)

	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return errors.New("fail to open the file")
	}

	oldstring := string(buff)
	var newstring string
	if location < len(oldstring) {
		newstring += oldstring[:location]
		newstring += string(content)
		newstring += oldstring[location:]
	} else if location == len(oldstring) {
		newstring += oldstring
		newstring += string(content)
	} else {
		newstring += oldstring
		for i := 0; i < location-len(buff); i++ {
			newstring += " "
		}
		newstring += string(content)
	}
	newbuff := []byte(newstring)
	//get the aguid to update the vguid

	//aguid := pc.Table[path1] //get the aguid of the dir
	hasher := md5.New()
	hasher.Write([]byte(path1))
	aguid := hex.EncodeToString(hasher.Sum(nil))

	inVGUID, _ := pc.ReadVguid(aguid)
	inBYTE, _ := pc.TClient.Get(inVGUID)
	inode, _ := puddlestore.DecodeInode(inBYTE)
	inode.IndirectBlock = "" //delete the connection between inode and its indirectblock

	datablocks := puddlestore.CreateDataBlock(newbuff)
	idList := make([]string, 0)
	for _, db := range datablocks {
		dbVGUID := uuid.New().String()
		dbBYTE, _ := puddlestore.EncodeDataBlock(db)

		// tclients, _ := pc.GetTClients()
		// for i := 0; i < 3; i++ {
		// 	pc.TClient = tclients[i]
		// 	pc.TClient.Store(dbVGUID, dbBYTE)
		//
		// }
		addrs, _ := pc.GetTClients()
		for _, addr := range addrs {
			tclient, err := tapestryclient.Connect(addr)
			if err != nil {
				fmt.Printf("fail to connect to a client: %v\n", err)
				return err
			}
			pc.TClient = tclient
			pc.TClient.Store(dbVGUID, dbBYTE)
		}

		idList = append(idList, dbVGUID)
	}

	ib := puddlestore.CreateIndirectBlock(idList)
	ibVGUID := uuid.New().String()
	ibBYTE, _ := puddlestore.EncodeIndirectBlock(ib)

	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(ibVGUID, ibBYTE)
	// }
	addrs, _ := pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(ibVGUID, ibBYTE)
	}

	inode.IndirectBlock = ibVGUID
	inode.Size = uint64(len(newbuff))
	pc.DeleteVguid(aguid) //delete the old vguid of inode
	newVGUID, _ := pc.CreateVguid(aguid)
	inBYTE, _ = puddlestore.EncodeInode(inode)
	// tclients, _ = pc.GetTClients()
	// for i := 0; i < 3; i++ {
	// 	pc.TClient = tclients[i]
	// 	pc.TClient.Store(newVGUID, inBYTE) //UPDATE
	// }
	addrs, _ = pc.GetTClients()
	for _, addr := range addrs {
		tclient, err := tapestryclient.Connect(addr)
		if err != nil {
			fmt.Printf("fail to connect to a client: %v\n", err)
			return err
		}
		pc.TClient = tclient
		pc.TClient.Store(newVGUID, inBYTE) //UPDATE
	}

	//create new datablock and indirectblock and generate new vguid
	//update inode indirectblock and Size
	//store to tapestry

	return nil
}

func (pc *PuddleClient) Disp(filename string) (err error) {
	err = pc.RWFilehelp(filename)
	if err != nil {
		return err
	}
	//var ok bool
	ok := pc.CheckExist(filename)
	if !ok {
		return errors.New("File doesn't exist!")
	}

	if pc.Path == "/" {
		filename = pc.Path + filename
		// path = pc.Path + filename
	} else {
		filename = pc.Path + "/" + filename
		// path = pc.Path"/"+filename
	}
	// tclients, _ := pc.GetTClients()
	// pc.TClient = tclients[0]
	addrs, _ := pc.GetTClients()
	tclient, err := tapestryclient.Connect(addrs[0])
	if err != nil {
		fmt.Printf("fail to connect to a client: %v\n", err)
		return err
	}
	pc.TClient = tclient

	//aguid := pc.Table[filename]
	hasher := md5.New()
	hasher.Write([]byte(filename))
	aguid := hex.EncodeToString(hasher.Sum(nil))

	vguid, _ := pc.ReadVguid(aguid)
	inodeB, _ := pc.TClient.Get(vguid)
	inode, _ := puddlestore.DecodeInode(inodeB)
	if inode.Type == puddlestore.DIR {
		return errors.New("Not a file!")
	}
	ibVguid := inode.IndirectBlock
	ibByte, _ := pc.TClient.Get(ibVguid)
	ib, _ := puddlestore.DecodeIndirectBlock(ibByte)
	idList := ib.List
	var content string
	for _, vguid := range idList {
		tmpByte, _ := pc.TClient.Get(vguid)
		tmp, _ := puddlestore.DecodeDataBlock(tmpByte)
		content += string(tmp.Data)
	}
	fmt.Printf("%v\n", content)
	return nil
}

func (pc *PuddleClient) Ls() (list []string, err error) {
	list = make([]string, 0)
	//aguid := pc.Table[pc.Path]
	hasher := md5.New()
	hasher.Write([]byte(pc.Path))
	aguid := hex.EncodeToString(hasher.Sum(nil))

	inodeVguid, _ := pc.ReadVguid(aguid)
	// tclients, _ := pc.GetTClients()
	// pc.TClient = tclients[0]
	addrs, _ := pc.GetTClients()
	tclient, err := tapestryclient.Connect(addrs[0])
	if err != nil {
		fmt.Printf("fail to connect to a client: %v\n", err)
		return nil, err
	}
	pc.TClient = tclient
	inodeB, _ := pc.TClient.Get(inodeVguid)
	inode, _ := puddlestore.DecodeInode(inodeB)
	ibVguid := inode.IndirectBlock
	ibByte, _ := pc.TClient.Get(ibVguid)
	ib, _ := puddlestore.DecodeIndirectBlock(ibByte)
	idList := ib.List
	for _, aguid := range idList {
		vguid, _ := pc.ReadVguid(aguid)
		tmpByte, _ := pc.TClient.Get(vguid)
		tmp, _ := puddlestore.DecodeInode(tmpByte)
		list = append(list, tmp.Name)
	}
	return list, nil
}

func (pc *PuddleClient) Pwd() (path string, err error) {
	return pc.Path, nil
}

func (pc *PuddleClient) CdRelative(path string) (err error) {
	steps := make([]string, 0)
	for i, char := range path {
		if char != '/' {
			continue
		} else {
			tmp := path[:i]
			steps = append(steps, tmp)
			path = path[i+1:]
		}
	}
	steps = append(steps, path)
	for _, step := range steps {
		err := pc.Cdhelp(step)
		if err != nil {
			return errors.New("Dir doesn't exist!")
		}

	}
	return nil
}

func (pc *PuddleClient) CdAbsolute(path string) (err error) {
	if path == "/" {
		pc.Path = "/"
		return nil
	}
	path = path[1:]
	originPath := pc.Path // REMEMBER ORIGIN PATH
	pc.Path = "/"         // GO BACK TO ROOT PATH
	steps := make([]string, 0)
	for i, char := range path {
		if char != '/' {
			continue
		} else {
			tmp := path[:i]
			steps = append(steps, tmp)
			path = path[i+1:]
		}
	}
	steps = append(steps, path)
	for _, step := range steps {
		err := pc.Cdhelp(step)
		if err != nil {
			pc.Path = originPath
			return errors.New("Dir doesn't exist!")
		}

	}
	return nil
}

func (pc *PuddleClient) Cdhelp(path string) (err error) {
	// A helper function which takes in relative path.
	//var ok bool
	var dirPath string
	if pc.Path == "/" {
		dirPath = pc.Path + path
	} else {
		dirPath = pc.Path + "/" + path
	}
	ok := pc.CheckExist(path)
	if !ok {
		return errors.New("File doesn't exist!")
	}
	// tclients, _ := pc.GetTClients()
	// pc.TClient = tclients[0]
	addrs, _ := pc.GetTClients()
	tclient, err := tapestryclient.Connect(addrs[0])
	if err != nil {
		fmt.Printf("fail to connect to a client: %v\n", err)
		return err
	}
	pc.TClient = tclient
	//aguid := pc.Table[dirPath]
	hasher := md5.New()
	hasher.Write([]byte(dirPath))
	aguid := hex.EncodeToString(hasher.Sum(nil))

	vguid, _ := pc.ReadVguid(aguid)
	inodeB, _ := pc.TClient.Get(vguid)
	inode, _ := puddlestore.DecodeInode(inodeB)
	if inode.Type == puddlestore.FILE {
		return errors.New("Not a directory!")
	}
	pc.Path = dirPath
	return nil
}

func (pc *PuddleClient) GetTClients() (addrList []string, err error) {
	var hosts = []string{"0.0.0.0"}
	//make a zookeeper connection
	zkConn, _, err := zk.Connect(hosts, time.Second*5)
	if err != nil {
		return nil, err
	}
	isExit, _, _ := zkConn.Exists("/tapestry")
	if !isExit {
		return nil, errors.New("something wrong in the puddleclient Create function")
	}
	children, _, err := zkConn.Children("/tapestry")
	if err != nil {
		return nil, err
	}
	len := len(children)
	if len == 0 { //some rnodes exists
		return nil, errors.New("no child in zookeeper, cannot continue to connect to client")
	}
	list := rand.Perm(len)
	idx := list[0]

	rAddr0, _, err := zkConn.Get("/tapestry/" + children[idx])
	if err != nil {
		return nil, err
	}
	// TClient0, err := tapestryclient.Connect(string(rAddr0))
	// if err != nil {
	// 	return nil, errors.New("fail to connect to client")
	// }
	// TClients = append(TClients, TClient0)
	addrList = append(addrList, string(rAddr0[:]))

	idx = list[1]
	rAddr1, _, err := zkConn.Get("/tapestry/" + children[idx])
	if err != nil {
		return nil, err
	}
	// TClient1, err := tapestryclient.Connect(string(rAddr1))
	// if err != nil {
	// 	return nil, errors.New("fail to connect to client")
	// }
	// TClients = append(TClients, TClient1)
	addrList = append(addrList, string(rAddr1[:]))

	idx = list[2]
	rAddr2, _, err := zkConn.Get("/tapestry/" + children[idx])
	if err != nil {
		return nil, err
	}
	// TClient2, err := tapestryclient.Connect(string(rAddr2))
	// if err != nil {
	// 	return nil, errors.New("fail to connect to client")
	// }
	// TClients = append(TClients, TClient2)
	addrList = append(addrList, string(rAddr2[:]))
	return addrList, err
}

func (pc *PuddleClient) RemoveTnodes(num string) (children []string, err error) {
	newlist := []tapestry.Node{}
	n, _ := strconv.Atoi(num)
	for i, node := range pc.TNode {
		if i == n {
			err := node.Leave()
			if err != nil {
				return children, err
			}
		} else {
			newlist = append(newlist, node)
		}
	}
	pc.TNode = newlist
	children, _, err = pc.Zk.Conn.Children("/tapestry")
	if err != nil {
		return children, err
	}
	return children, nil
}

func (pc *PuddleClient) AddTnode(port string) (children []string, err error) {
	p, _ := strconv.Atoi(port)
	_, err = tapestry.Start(p, "")
	if err != nil {
		return children, err
	}
	children, _, err = pc.Zk.Conn.Children("/tapestry")
	if err != nil {
		return children, err
	}
	return children, nil
}

func (pc *PuddleClient) RWFilehelp(path string) error {
	steps := make([]string, 0)
	l := 0
	for i := 0; i < len(path); i++ {
		if path[i] != '/' {
			continue
		} else {
			tmp := path[l:i]
			steps = append(steps, tmp)
			l = i + 1
		}
	}

	originpath := pc.Path
	for _, step := range steps {
		err := pc.CdRelative(step)
		if err != nil {
			pc.CdAbsolute(originpath)
			return errors.New("File doesn't exist(cannot cd)!")
		}
	}
	pc.CdAbsolute(originpath)
	return nil
}

func (pc *PuddleClient) CheckExist(filename string) bool {
	hasher := md5.New()
	hasher.Write([]byte(pc.Path))
	aguid := hex.EncodeToString(hasher.Sum(nil))

	dirVguid, _ := pc.ReadVguid(aguid)
	inodeB, _ := pc.TClient.Get(dirVguid)
	inode, _ := puddlestore.DecodeInode(inodeB)
	ibVguid := inode.IndirectBlock
	ibB, _ := pc.TClient.Get(ibVguid)
	ib, _ := puddlestore.DecodeIndirectBlock(ibB)
	// ib.List: AGUIDs
	names := make(map[string]int)
	for _, ag := range ib.List {
		vg, _ := pc.ReadVguid(ag)
		inBtemp, _ := pc.TClient.Get(vg)
		intemp, _ := puddlestore.DecodeInode(inBtemp)
		names[intemp.Name] = 1
	}
	// Find filename in names
	_, ok := names[filename]

	if ok {
		return true
	} else {
		return false
	}

}
