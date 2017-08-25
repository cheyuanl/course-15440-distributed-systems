package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type StringEntry struct {
	value       string
	expiredTime time.Time
}

type ListEntry struct {
	value       []string
	expiredTime time.Time
}

type libstore struct {
	myAddress string
	leaseMode LeaseMode

	ssClient  *rpc.Client
	ssServers []storagerpc.Node

	stringCache      map[string]StringEntry
	stringCacheMutex sync.Mutex
	listCache        map[string]ListEntry
	listCacheMutex   sync.Mutex

	queryCount      map[string]int
	queryCountMutex sync.Mutex
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	ls := new(libstore)

	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))

	ssClient, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	ls.ssClient = ssClient

	for {
		args := &storagerpc.GetServersArgs{}
		var reply storagerpc.GetServersReply
		err = ls.ssClient.Call("StorageServer.GetServers", args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			ls.ssServers = reply.Servers
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	ls.myAddress = myHostPort
	ls.leaseMode = mode
	ls.stringCache = make(map[string]StringEntry)
	ls.listCache = make(map[string]ListEntry)
	ls.queryCount = make(map[string]int)

	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	ls.stringCacheMutex.Lock()
	value, exists := ls.stringCache[key]
	if exists {
		if value.expiredTime.After(time.Now()) {
			ls.stringCacheMutex.Unlock()
			return value.value, nil
		} else {
			delete(ls.stringCache, key)
		}
	}
	ls.stringCacheMutex.Unlock()

	wantLease := ls.GetWantLease(key)
	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.myAddress}
	var reply storagerpc.GetReply
	err := ls.ssClient.Call("StorageServer.Get", args, &reply)

	if reply.Status != storagerpc.OK {
		return reply.Value, errors.New(fmt.Sprintf("Get Reply Status Error: %v", reply.Status))
	}

	// save into cache
	ls.stringCacheMutex.Lock()
	ls.stringCache[key] = StringEntry{reply.Value, time.Now().Add(time.Duration(reply.Lease.ValidSeconds) * time.Second)}
	ls.stringCacheMutex.Unlock()
	go func() {
		time.Sleep(time.Duration(reply.Lease.ValidSeconds) * time.Second)
		ls.stringCacheMutex.Lock()
		value, exists := ls.stringCache[key]
		if exists && value.expiredTime.Before(time.Now()) {
			delete(ls.stringCache, key)
		}
		ls.stringCacheMutex.Unlock()
	}()
	ls.UpdateQueryCount(key)

	return reply.Value, err
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	err := ls.ssClient.Call("StorageServer.Put", args, &reply)

	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("Put Reply Status Error: %v", reply.Status))
	}
	return err
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	err := ls.ssClient.Call("StorageServer.Delete", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("Delete Reply Status Error: %v", reply.Status))
	}
	return err
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.listCacheMutex.Lock()
	value, exists := ls.listCache[key]
	if exists {
		if value.expiredTime.After(time.Now()) {
			ls.listCacheMutex.Unlock()
			return value.value, nil
		} else {
			delete(ls.listCache, key)
		}
	}
	ls.listCacheMutex.Unlock()

	wantLease := ls.GetWantLease(key)
	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.myAddress}
	var reply storagerpc.GetListReply
	err := ls.ssClient.Call("StorageServer.GetList", args, &reply)

	if reply.Status != storagerpc.OK {
		return reply.Value, errors.New(fmt.Sprintf("GetList Reply Status Error: %v", reply.Status))
	}

	// save into cache
	ls.listCacheMutex.Lock()
	ls.listCache[key] = ListEntry{reply.Value, time.Now().Add(time.Duration(reply.Lease.ValidSeconds) * time.Second)}
	ls.listCacheMutex.Unlock()
	go func() {
		time.Sleep(time.Duration(reply.Lease.ValidSeconds) * time.Second)
		ls.listCacheMutex.Lock()
		value, exists := ls.listCache[key]
		if exists && value.expiredTime.Before(time.Now()) {
			delete(ls.listCache, key)
		}
		ls.listCacheMutex.Unlock()
	}()
	ls.UpdateQueryCount(key)

	return reply.Value, err
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	err := ls.ssClient.Call("StorageServer.RemoveFromList", args, &reply)

	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("RemoveFromList Reply Status Error: %v", reply.Status))
	}
	return err
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	err := ls.ssClient.Call("StorageServer.AppendToList", args, &reply)

	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("AppendToList Reply Status Error: %v", reply.Status))
	}
	return err
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.stringCacheMutex.Lock()
	delete(ls.stringCache, args.Key)
	ls.stringCacheMutex.Unlock()

	ls.listCacheMutex.Lock()
	delete(ls.listCache, args.Key)
	ls.listCacheMutex.Unlock()

	reply.Status = storagerpc.OK

	return nil
}

func (ls *libstore) GetWantLease(key string) bool {
	var wantLease bool
	if ls.leaseMode == Never {
		wantLease = false
	} else if ls.leaseMode == Always {
		wantLease = true
	} else if ls.leaseMode == Normal {
		ls.queryCountMutex.Lock()
		count, exists := ls.queryCount[key]
		ls.queryCountMutex.Unlock()

		if exists && count >= storagerpc.QueryCacheThresh {
			wantLease = true
		} else {
			wantLease = false
		}
	}

	return wantLease
}

func (ls *libstore) UpdateQueryCount(key string) {
	ls.queryCountMutex.Lock()
	count, exists := ls.queryCount[key]
	if exists {
		ls.queryCount[key] = count + 1
	} else {
		ls.queryCount[key] = 1
	}
	ls.queryCountMutex.Unlock()
	go func() {
		time.Sleep(storagerpc.QueryCacheSeconds * time.Second)
		ls.queryCountMutex.Lock()
		count, _ = ls.queryCount[key]
		ls.queryCount[key] = count - 1
		ls.queryCountMutex.Unlock()
	}()
}
