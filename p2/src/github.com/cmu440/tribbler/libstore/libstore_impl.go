package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	myAddress string
	mode      LeaseMode

	ssClient  *rpc.Client
	ssServers []storagerpc.Node
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
		time.Sleep(1000 * time.Millisecond)
	}

	ls.myAddress = myHostPort
	ls.mode = mode

	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	wantLease := false
	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.myAddress}
	var reply storagerpc.GetReply
	err := ls.ssClient.Call("StorageServer.Get", args, &reply)

	if reply.Status != storagerpc.OK {
		return reply.Value, errors.New(fmt.Sprintf("Get Reply Status Error: %v", reply.Status))
	}
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
	wantLease := false
	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.myAddress}
	var reply storagerpc.GetListReply
	err := ls.ssClient.Call("StorageServer.GetList", args, &reply)

	if reply.Status != storagerpc.OK {
		return reply.Value, errors.New(fmt.Sprintf("GetList Reply Status Error: %v", reply.Status))
	}
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
	return errors.New("not implemented")
}
