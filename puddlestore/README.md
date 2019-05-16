# Puddlestore

### A-level Feature
#### 1. Caching
We create a struct called Cache and it stores a RemoteNode and a timer. We then added a map of the ID of an object and its corresponding Cache to the tapestry Node struct. When we store an object, we will call the method findRoot. And here in the findRoot we would first check the cache of the Node struct to find its root, and will directly get its root if there is one. If not, the original method will be performed and the result will be added to Cache. For the timer, if an object is not asked for after a given time frame, its Cache will be removed automatically.
#### 2. Salting
When we store a key, we add salt to it so that it can be stored with 3 different keys. And when we perform lookup and get, we would search for the 3 keys and return success if we successfully reach any of them.

### How to use
First, make sure you have started a Zookeeper server. If you dont't have a Zookeeper server, you can download it [here](https://archive.apache.org/dist/zookeeper/), the stable version is recommended. Also, you need to create a **zoo.cfg** file in config folder.

Secondly, go to ./puddlestore/cmd/ folder and run the following command to start a puddle client:

```
go run .\CLI.go -c 0.0.0.0
```

where **-c** means connect, and you can use any address you like to start the client.

If it is the first time you start the client, you will find that no child in Zookeeper. You should create raft nodes and tapestry nodes with an dd number first:

```
create raft 3
create tapestry 5
```
![image](https://github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/blob/backup/puddlestore/image/first_client.png)
You can open several clients and you will find that all the raft nodes and tapestry nodes have been stored in Zookeeper. At this time, however, you are not allowed to create raft nodes or tapestry nodes again. Now, you can use all other commands to play with our file system! You will find the sychronization among all these clients.
![image](https://github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/blob/backup/puddlestore/image/new_client.png)
### Test
We wrote all the major functions which perform basic file and directory operations in the file puddleclient/puddleclient.go. As it is separate from raft and tapestry module and we used TA implementation of these two modules in which case we assume is right, we only tested the functions that are written by ourselves. The logic is simple, run each function with both correct and maybe incorrect inputs to maximize the coverage. The overall coverage is about 83%. In order to test whether file data is still available when Tapestry node leaves or a new node is added, we added **addtap** and **removetap** commands.

### Commands
```
create <node type> <num>              create raft nodes or tapestry node with a given number
mk <filename>                         make a new file    
mkdir <dirname>                       make a new directory
write <filename> <pos> <content>      write content to a file at a specified position
read <filename> <pos>                 read a file from a specified position
disp <filename>                       display the content of a file to the screen
rm <filename>                         delete a file
rmdir <dirname>                       delete a directory
cd <path>                             change directory to the specified path
ls                                    list the content of the current directory
pwd                                   show the current path
addtap <port>                         add a new tapestry node with a given port
removetap <index>                     remove a tapestry node with a given 0-based index
```
