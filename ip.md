# IP Project

### Computer Network, Spring 2022

## Group

Guangfeng Xu,<br /> 
Zijian Chen

## Introduction

In this project, we construct a Virtual IP Network using UDP as the link layer. The network will support dynamic routing. Each node will be configured with its (virtual) links at startup and support the activation and deactivation of those links at run time. We build a simple routing protocol over these links to dynamically update the nodes’ routing tables so that they can communicate over the virtual topology


## Technical Features

### 1. Usage

- In project directory, run `make`, which will create a executable file `ref_node`
- Or in `cmd/ref_node` directory, which is the directory we usually use for writing our project, run `go build ref_node.go`, which will create a executable file `ref_node`
- To run the project , we provide 2 options:<br />
  Running normally: `./refnode -p=[filname].lnx` or `./refnode -parse=[filname].lnx`<br />
  With debug mode: `./refnode -p=[filname].lnx -d=true` or `./refnode -parse=[filname].lnx -debug=true`
  


### 2. Driver
Each time you run this project, we will read a lnx file and create a node struct for this file, which contains interfaces, routing table and other useful stuff:
``` go
    type Node struct {
	udpAddr      *net.UDPAddr
	interfaces   *Interfaces
	conn         *net.UDPConn
	handlers     map[int]Handler
	routingTable *RoutingTable
    }
```

### 3. Abstract Link Layer

We use UDP as the link layer for this project. Each node will create an interface for every line in its links file — those interfaces will be implemented by a UDP socket. All of the virtual link layer frames it sends are directly encapsulated as payloads of UDP packets that will be sent over these sockets.
The Interface structs are as follows:
``` go
    type Interface struct {
        index        int
        udpAddr      *net.UDPAddr
        localIpAddr  *net.IPAddr
        remoteIpAddr *net.IPAddr
        status       bool
        // We don't need conn, since we can use the local udpconn as all the conn
        // conn         *net.UDPConn
    }

    type Interfaces struct {
        interfaces []Interface
        mutex      sync.Mutex     // We need a mutex to protect Interfaces since it won't be changed after created
        ipMap      map[string]int // map localIp to index
    }
```
### 4. Forwarding

We design a network layer that sends and receives IP packets using the link layer. Overall, the network layer will read packets from your link layer, then decide what to do with the packet: local delivery or forwarding. 
Based on this, The routing table structs are as follows:
``` go
    type RoutingTable struct {
        tableMap map[string]*RoutingTableEntry
        mutex    sync.Mutex
    }

    type RoutingTableEntry struct {
        localInterface *Interface
        locIp          string
        remIp          string
        cost           uint
        timestamp      time.Time
    }
``` 

## Interface to higher layers
We need an interface between network layer and upper layers for local delivery. In this project, some of our packets need to be handed off to RIP; others, which we will call “test packets” sent using the send comand, will simply be printed. So we design and implement an interface that allows an upper layer to register a handler for a given protocol number:
```go
    type Handler func(data []byte, node *Node)

    func RIPHandler(data []byte, node *Node) {
        
    }

    func BasicHandler(data []byte, node *Node) {
        
    }

    func (node *Node) NetRegisterHandler(key int, value Handler) {
        
    }
```

## Routing with RIP
We implement routing using a modified version of RIP protocol, which is defined by RFC24533. 
The rip structs are as follows:
``` go
    type RIPEntry struct {
        cost    uint32
        address uint32
        mask    uint32
    }

    type RIPMessage struct {
        command    uint16
        numEntries uint16
        entries    []RIPEntry
    }
``` 
Once a node comes online, it must send a request on each of its interfaces. Each node must send periodic updates to all of its interfaces every 5 seconds. A routing entry should expire if it has not been refreshed in 12 seconds6. If a link goes down, then the network should be able to recover by finding different routes to nodes that went through that link. 
To implement it, we have a timestamp attribute in each RoutingTableEntry, and to check expiration, we have the function:
``` go
    func (t *RoutingTable) CheckExpired() []string {	
        t.mutex.Lock()
        defer t.mutex.Unlock()
        timeNow := time.Now()
        ret := make([]string, 0)
        for _, value := range t.tableMap {
            if value.localInterface != nil {
                // we don't delete ourself
                timeDuration := timeNow.Sub(value.timestamp)
                if timeDuration.Seconds() >= 12 {
                    ret = append(ret, value.remIp)
                }
            }
        }
        return ret
    }
``` 
We also implement split horizon with poisoned reverse:
```go
	if entry.localInterface != nil && localInterface.equals(entry.localInterface) {
		ripEntry.cost = 16
	} else {
		ripEntry.cost = uint32(entry.cost)
	}
```
As well as triggered updates, Triggered updates do not contain the entire routing table, just the routes that are updated:
```go
    func (n *Node) sendTriggeredUpdateRIP(entry *RoutingTableEntry) {
        for _, v := range n.interfaces.interfaces {
            if v.status == true {
                n.sendTriggeredUpdateRIPtoInterface(v, entry)
            }
        }
    }

    func (n *Node) sendTriggeredUpdateRIPtoInterface(localInterface Interface, entry *RoutingTableEntry) {

    }
```