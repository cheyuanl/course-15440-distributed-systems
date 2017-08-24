package storageserver

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type Nodes []storagerpc.Node

func (slice Nodes) Len() int {
	return len(slice)
}

func (slice Nodes) Less(i, j int) bool {
	return slice[i].NodeID < slice[j].NodeID
}

func (slice Nodes) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type storageServer struct {
	servers      Nodes
	serversMutex sync.Mutex

	data      map[string]string
	dataMutex sync.Mutex

	node     storagerpc.Node
	numNodes int
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := new(storageServer)
	ss.data = make(map[string]string)
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	ss.node = storagerpc.Node{fmt.Sprintf("%s:%d", hostname, port), nodeID}
	ss.numNodes = numNodes
	ss.servers = Nodes{ss.node}

	// start rpc service
	rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	go http.Serve(l, nil)

	if len(masterServerHostPort) > 0 {
		// connect to the master
		var ssClient *rpc.Client
		ssClient, err = rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}

		// keep registering until the master returns OK
		args := &storagerpc.RegisterArgs{ServerInfo: ss.node}
		var reply storagerpc.RegisterReply
		for {
			err = ssClient.Call("StorageServer.Get", args, &reply)
			if err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers
				break
			}
			time.Sleep(1000 * time.Millisecond)
		}
	} else {
		if numNodes > 1 {
			for {
				ss.serversMutex.Lock()
				if len(ss.servers) == ss.numNodes {
					ss.serversMutex.Unlock()
					break
				}
				ss.serversMutex.Unlock()
				time.Sleep(1000 * time.Millisecond)
			}
		}
	}

	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.serversMutex.Lock()
	defer ss.serversMutex.Unlock()

	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		return nil
	}

	for _, server := range ss.servers {
		if server == args.ServerInfo {
			reply.Status = storagerpc.NotReady
			return nil
		}
	}
	ss.servers = append(ss.servers, args.ServerInfo)
	if len(ss.servers) == ss.numNodes {
		sort.Sort(ss.servers)
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.serversMutex.Lock()
	defer ss.serversMutex.Unlock()

	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) checkKeyInRange(key string) bool {
	server := libstore.GetServerForKey(ss.servers, key)
	return server == ss.node
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

	if !ss.checkKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	value, exists := ss.data[args.Key]
	if exists {
		reply.Status = storagerpc.OK
		reply.Value = value
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

	if !ss.checkKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	_, exists := ss.data[args.Key]
	if exists {
		delete(ss.data, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

	if !ss.checkKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	rawValue, exists := ss.data[args.Key]
	if exists {
		var list []string
		json.Unmarshal([]byte(rawValue), &list)
		reply.Status = storagerpc.OK
		reply.Value = list
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

	if !ss.checkKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.data[args.Key] = args.Value
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

	if !ss.checkKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	rawValue, exists := ss.data[args.Key]
	var list []string
	if exists {
		json.Unmarshal([]byte(rawValue), &list)
		for _, item := range list {
			if item == args.Value {
				reply.Status = storagerpc.ItemExists

				return nil
			}
		}
		list = append(list, args.Value)
	} else {
		list = []string{args.Value}
	}
	newByteValue, _ := json.Marshal(list)
	ss.data[args.Key] = string(newByteValue)
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

	if !ss.checkKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	rawValue, exists := ss.data[args.Key]
	if exists {
		var list []string
		json.Unmarshal([]byte(rawValue), &list)
		for i, item := range list {
			if item == args.Value {
				list[i] = list[len(list)-1]
				list = list[:len(list)-1]
				newByteValue, _ := json.Marshal(list)
				ss.data[args.Key] = string(newByteValue)

				reply.Status = storagerpc.OK

				return nil
			}
		}
	}

	reply.Status = storagerpc.ItemNotFound

	return nil
}
