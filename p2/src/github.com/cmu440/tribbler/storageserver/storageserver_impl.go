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

	leases              map[string](map[string]int64)
	leasesMutex         sync.Mutex
	leasesRevoking      map[string]bool
	leasesRevokingMutex sync.Mutex
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
	ss.leases = make(map[string](map[string]int64))
	ss.leasesRevoking = make(map[string]bool)

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
			err = ssClient.Call("StorageServer.RegisterServer", args, &reply)
			if err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers
				break
			}
			time.Sleep(100 * time.Millisecond)
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
				time.Sleep(100 * time.Millisecond)
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

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.CheckKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.dataMutex.Lock()
	value, exists := ss.data[args.Key]
	ss.dataMutex.Unlock()

	if exists {
		reply.Status = storagerpc.OK
		reply.Value = value
		if args.WantLease {
			reply.Lease = ss.GrantLease(args.Key, args.HostPort)
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.CheckKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.dataMutex.Lock()
	_, exists := ss.data[args.Key]
	if exists {
		delete(ss.data, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.dataMutex.Unlock()

	if reply.Status == storagerpc.OK {
		err := ss.RevokeLeases(args.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.CheckKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.dataMutex.Lock()
	rawValue, exists := ss.data[args.Key]
	ss.dataMutex.Unlock()

	if exists {
		var list []string
		json.Unmarshal([]byte(rawValue), &list)
		reply.Status = storagerpc.OK
		reply.Value = list

		if args.WantLease {
			reply.Lease = ss.GrantLease(args.Key, args.HostPort)
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.CheckKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	err := ss.RevokeLeases(args.Key)
	if err != nil {
		return err
	}

	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

	ss.data[args.Key] = args.Value

	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.CheckKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	err := ss.RevokeLeases(args.Key)
	if err != nil {
		return err
	}

	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()

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
	if !ss.CheckKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	err := ss.RevokeLeases(args.Key)
	if err != nil {
		return err
	}

	ss.dataMutex.Lock()
	defer ss.dataMutex.Unlock()
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

func (ss *storageServer) CheckKeyInRange(key string) bool {
	server := libstore.GetServerForKey(ss.servers, key)
	return server == ss.node
}

func (ss *storageServer) GrantLease(key string, hostPort string) storagerpc.Lease {
	ss.leasesRevokingMutex.Lock()
	status, exists := ss.leasesRevoking[key]
	ss.leasesRevokingMutex.Unlock()
	if exists && status {
		return storagerpc.Lease{false, 0}
	}

	ss.leasesMutex.Lock()
	defer ss.leasesMutex.Unlock()
	_, leaseExists := ss.leases[key]
	if !leaseExists {
		ss.leases[key] = make(map[string]int64)
	}
	ss.leases[key][hostPort] = time.Now().Unix()
	return storagerpc.Lease{true, storagerpc.LeaseSeconds}
}

func IsExpired(timestamp int64) bool {
	timeElapsed := time.Since(time.Unix(timestamp, 0))
	return timeElapsed.Seconds() >= storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds
}

func LeftTime(timestamp int64) time.Duration {
	expiredTime := time.Unix(timestamp, 0).Add((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second)
	return expiredTime.Sub(time.Now())
}

func (ss *storageServer) RevokeLeases(key string) error {
	for {
		ss.leasesRevokingMutex.Lock()
		status, exists := ss.leasesRevoking[key]
		if exists && status {
			ss.leasesRevokingMutex.Unlock()
			time.Sleep(1000 * time.Millisecond)
		} else {
			ss.leasesRevoking[key] = true
			ss.leasesRevokingMutex.Unlock()
			break
		}
	}

	ss.leasesMutex.Lock()
	holders, exists := ss.leases[key]
	ss.leasesMutex.Unlock()
	if exists {
		size := len(holders)
		doneChan := make(chan bool, size)

		for holderAddress, timestamp := range holders {
			go func() {
				holderClient, _ := rpc.DialHTTP("tcp", holderAddress)
				if !IsExpired(timestamp) {
					revokeLeaseChan := make(chan bool, 1)
					leftTime := LeftTime(timestamp)
					go func() {
						for {
							args := &storagerpc.RevokeLeaseArgs{Key: key}
							var reply storagerpc.RevokeLeaseReply
							holderClient.Call("LeaseCallbacks.RevokeLease", args, &reply)
							if reply.Status == storagerpc.OK {
								revokeLeaseChan <- true
								break
							}
						}
					}()
					select {
					case <-revokeLeaseChan:
					case <-time.After(leftTime):
						break
					}
				}
				doneChan <- true
			}()
		}

		for i := 0; i < size; i++ {
			<-doneChan
		}

		ss.leasesMutex.Lock()
		delete(ss.leases, key)
		ss.leasesMutex.Unlock()
	}

	ss.leasesRevokingMutex.Lock()
	ss.leasesRevoking[key] = false
	ss.leasesRevokingMutex.Unlock()

	return nil
}
