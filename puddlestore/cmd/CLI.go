/*
* Purpose: a CLI for puddle store
 */

package main

import (
	"flag"
	"fmt"
	"strconv"
	"unicode"

	"github.com/abiosoft/ishell"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/puddleclient"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/raft/raft"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/tapestry/tapestry"
)

func main() {
	raft.SuppressLoggers()
	tapestry.SuppressLoggers()
	var addr string
	// var host []string
	addrHelpString := "Address of an online node of the puddlestore to connect to."
	flag.StringVar(&addr, "connect", "", addrHelpString)
	flag.StringVar(&addr, "c", "", addrHelpString)

	flag.Parse()

	// Validate address of Raft node
	if addr == "" {
		fmt.Println("Usage: client -c <addr>\nYou must specify an address for the puddle client to connect to!")
		return
	}
	pc := puddleclient.CreateClient(addr)

	//---------------------------------------------------------------------//
	// Kick off shell
	shell := ishell.New()
	//create
	shell.AddCmd(&ishell.Cmd{
		Name: "create",
		Help: "create raft nodes or tapestry nodes inside zookeeper",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 2 {
				shell.Println("Usage: create </raft or /tapestry> <number>")
				return
			}
			nodetype := c.Args[0]
			s := c.Args[1]
			char := []rune(s)
			for _, r := range char {
				if !unicode.IsNumber(r) {
					shell.Println("the second parameter must be a number!")
					return
				}
			}

			num, _ := strconv.Atoi(s)
			children, err := pc.Create(nodetype, num)
			if err != nil {
				shell.Println(err.Error())
				return
			}
			shell.Printf("Create successfully! The children are: %v\n", children)

		},
	})

	//mk
	shell.AddCmd(&ishell.Cmd{
		Name: "mk",
		Help: "make a file in the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: mk <filename>")
				return
			}
			filename := c.Args[0]

			char := []rune(filename)
			if string(char[0]) == "/" { //judge the first character
				shell.Println("only relative path is accepted")
				return
			}
			for i, r := range char { //check invalid symbols
				if i == 0 { //because the first one is "/"
					continue
				} else if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
					shell.Println("// WARNING: invalid input!")
					return
				}
			}

			err := pc.Mk(filename)
			if err != nil {
				shell.Printf("something wrong when making a file: %v\n", err)
				return
			}
			return

		},
	})

	//mkdir
	shell.AddCmd(&ishell.Cmd{
		Name: "mkdir",
		Help: "make a directory in the current path",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: mkdir <dirname>")
				return
			}

			dirname := c.Args[0]
			char := []rune(dirname)
			if string(char[0]) == "/" {
				shell.Println("only relative path is accepted")
				return
			}
			for i, r := range char { //check invalid symbols
				if i == 0 { //because the first one is "/"
					continue
				} else if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
					shell.Println("// WARNING: invalid input!")
					return
				}
			}

			err := pc.Mkdir(dirname)
			if err != nil {
				shell.Printf("something wrong when making a directory: %v\n", err)
				return
			}
		},
	})

	//rm
	shell.AddCmd(&ishell.Cmd{
		Name: "rm",
		Help: "delete a file in the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: rm <filename>")
				return
			}

			filename := c.Args[0]
			char := []rune(filename)
			if string(char[0]) == "/" {
				shell.Println("only relative path is accepted")
				return
			}
			for i, r := range char { //check invalid symbols
				if i == 0 { //because the first one is "/"
					continue
				} else if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
					shell.Println("// WARNING: invalid input!")
					return
				}
			}
			err := pc.Rm(filename)
			if err != nil {
				shell.Println(err.Error())
				return
			}
		},
	})

	//rmdir
	shell.AddCmd(&ishell.Cmd{
		Name: "rmdir",
		Help: "delete a directory and all the files inside",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: rmdir <dirname>")
				return
			}

			dirname := c.Args[0]
			char := []rune(dirname)
			if string(char[0]) == "/" {
				shell.Println("only relative path is accepted")
				return
			}
			for i, r := range char { //check invalid symbols
				if i == 0 { //because the first one is "/"
					continue
				} else if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
					shell.Println("// WARNING: invalid input!")
					return
				}
			}

			err := pc.Rmdir(dirname)

			if err != nil {
				shell.Println(err.Error())
				return
			}
		},
	})

	//read
	shell.AddCmd(&ishell.Cmd{
		Name: "read",
		Help: "read file data begin at a certain location in current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 2 {
				shell.Println("Usage: read <filename> <location>")
				return
			}

			filename := c.Args[0]
			char := []rune(filename)
			if string(char[0]) == "/" {
				shell.Println("only relative path is accepted")
				return
			}
			s := c.Args[1]
			location, _ := strconv.Atoi(s)
			buff, err := pc.Read(filename, location)
			if err != nil {
				shell.Println(err.Error())
				return
			}
			shell.Print(string(buff), "\n")
		},
	})
	//write
	shell.AddCmd(&ishell.Cmd{
		Name: "write",
		Help: "write file data and  begin at a certain location in the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 3 {
				shell.Println("Usage: write <filename> <location> <content>")
				return
			}

			filename := c.Args[0]
			char := []rune(filename)
			if string(char[0]) == "/" {
				shell.Println("only relative path is accepted")
				return
			}

			s := c.Args[1]
			location, _ := strconv.Atoi(s)
			content := []byte(c.Args[2])
			err := pc.Write(filename, location, content)
			if err != nil {
				shell.Println(err.Error())
				return
			}
		},
	})
	//disp
	shell.AddCmd(&ishell.Cmd{
		Name: "disp",
		Help: "display the content of a file ",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: disp <filename>")
				return
			}

			filename := c.Args[0]
			char := []rune(filename)
			if string(char[0]) == "/" {
				shell.Println("only relative path is accepted")
				return
			}

			err := pc.Disp(filename)
			if err != nil {
				shell.Println(err.Error())
				return
			}
		},
	})
	//ls
	shell.AddCmd(&ishell.Cmd{
		Name: "ls",
		Help: "list all the files and directories in the current directory",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 0 {
				shell.Println("Usage: ls ")
				return
			}
			list, err := pc.Ls()
			if err != nil {
				shell.Println(err.Error())
				return
			} else {
				shell.Printf("list: %v\n", list)
			}
		},
	})
	//pwd
	shell.AddCmd(&ishell.Cmd{
		Name: "pwd",
		Help: "display the current path",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 0 {
				shell.Println("Usage: pwd")
				return
			}
			path, err := pc.Pwd()
			if err != nil {
				shell.Println(err.Error())
				return
			} else {
				shell.Printf("the current path is %v\n", path)
			}
		},
	})
	//cd
	shell.AddCmd(&ishell.Cmd{
		Name: "cd",
		Help: "enter the given path, only absolute path is accepted",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: cd <path>")
				return
			}

			path := c.Args[0]
			char := []rune(path)
			if string(char[0]) != "/" {
				err := pc.CdRelative(path)
				if err != nil {
					shell.Println(err.Error())
					return
				}
				return
			}

			if string(char[0]) == "/" { //absolute path
				err := pc.CdAbsolute(path)
				if err != nil {
					shell.Println(err.Error())
					return
				}
				return
			}

		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "removetap",
		Help: "remove a tapestry node in with a given index (0 based)",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: removetap <num>")
				return
			}
			num := c.Args[0]
			char := []rune(num)
			for _, r := range char {
				if !unicode.IsNumber(r) {
					shell.Println("// WARNING: invalid input!")
					return
				}
			}
			n, _ := strconv.Atoi(num)
			if n < 0 || n >= len(pc.TNode) {
				shell.Println("input number out of range!")
				return
			}
			children, _, err := pc.Zk.Conn.Children("/tapestry")
			if err != nil {
				return
			}
			shell.Printf("the current tap nodes in zookeeper are: %v\n", children)
			newchildren, err := pc.RemoveTnodes(num)
			if err != nil {
				shell.Println("fail to remove a tap node")
				return
			}
			shell.Printf("new tap node list in zookeeper: %v\n", newchildren)

		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "addtap",
		Help: "add a tapestry node with a given port (4 digits)",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: addtap <port>")
				return
			}

			port := c.Args[0]
			char := []rune(port)
			for _, r := range char {
				if !unicode.IsNumber(r) {
					shell.Println("// WARNING: invalid input")
					return
				}
			}

			children, _, err := pc.Zk.Conn.Children("/tapestry")
			if err != nil {
				return
			}
			shell.Printf("the current tap nodes in zookeeper are: %v\n", children)

			newchildren, err := pc.AddTnode(port)
			if err != nil {
				shell.Println("fail to add a tap node")
				return
			}
			shell.Printf("the new tap nodes in zookeeper are: %v\n", newchildren)
		},
	})

	shell.Println(shell.HelpText())
	shell.Run()
}
